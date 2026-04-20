import json
import os
from datetime import datetime
from typing import Any, Dict, List, Type

import bridge_orm_rs

from ..core import _MODEL_REGISTRY

MIGRATIONS_DIR = "migrations"
SCHEMA_SNAPSHOT = os.path.join(MIGRATIONS_DIR, "schema.json")


class MigrationEngine:
    def __init__(self, dialect: str = "sqlite"):
        self.dialect = dialect
        if not os.path.exists(MIGRATIONS_DIR):
            os.makedirs(MIGRATIONS_DIR)

    def load_snapshot(self) -> Dict[str, Any]:
        if os.path.exists(SCHEMA_SNAPSHOT):
            with open(SCHEMA_SNAPSHOT, "r") as f:
                return json.load(f)
        return {"tables": {}}

    def save_snapshot(self, snapshot: Dict[str, Any]):
        with open(SCHEMA_SNAPSHOT, "w") as f:
            json.dump(snapshot, f, indent=4)

    async def generate_migration(self, description: str = "auto_migration"):
        # 1. Introspect actual database state
        db_schema = await bridge_orm_rs.reflect_schema()
        db_tables = {t.name: t for t in db_schema}

        # 2. Get desired state from models
        model_tables = {}
        for table_name, model_cls in _MODEL_REGISTRY.items():
            model_tables[table_name] = model_cls.get_field_definitions()

        # 3. Diffing logic
        sql_statements = []
        warnings = []

        # Detect New Tables or Changes in Existing Tables
        for table_name, model_fields in model_tables.items():
            if table_name not in db_tables:
                sql = self._generate_create_table(table_name, model_fields)
                sql_statements.append(sql)
            else:
                # Table exists, check columns
                db_table = db_tables[table_name]
                db_column_names = {c.name for c in db_table.columns}
                
                # New Columns
                for field_name, field_type in model_fields.items():
                    if field_name not in db_column_names:
                        sql_type = bridge_orm_rs.resolve_type(field_type, self.dialect)
                        sql_statements.append(
                            f"ALTER TABLE {table_name} ADD COLUMN {field_name} {sql_type};"
                        )
                
                # Missing Columns in Model (Dropped or Manual)
                model_column_names = set(model_fields.keys())
                for db_col in db_table.columns:
                    if db_col.name not in model_column_names:
                        warnings.append(
                            f"Warning: Column '{db_col.name}' exists in database table '{table_name}' but is not defined in the model."
                        )

        # Detect Tables in DB but not in Models
        for db_table_name in db_tables:
            if db_table_name not in model_tables and not db_table_name.startswith("sqlite_"):
                warnings.append(
                    f"Warning: Table '{db_table_name}' exists in database but has no corresponding model."
                )

        if warnings:
            print("\nReconciliation Warnings:")
            for w in warnings:
                print(f"  - {w}")

        if not sql_statements:
            print("\nNo schema changes needed.")
            return

        # 4. Generate file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}_{description}.sql"
        filepath = os.path.join(MIGRATIONS_DIR, filename)

        with open(filepath, "w") as f:
            f.write("-- BridgeORM Reconciliation-based Migration\n")
            f.write(f"-- Generated: {datetime.now().isoformat()}\n\n")
            f.write("\n".join(sql_statements))

        print(f"\nCreated migration: {filepath}")
        print("Please review the SQL file before applying.")

    def _generate_create_table(self, table_name: str, fields: Dict[str, str]) -> str:
        column_defs = []
        for name, py_type in fields.items():
            sql_type = bridge_orm_rs.resolve_type(py_type, self.dialect)
            # Simple primary key logic for prototype: if field is 'id', make it PK
            if name == "id":
                sql_type += " PRIMARY KEY"
            column_defs.append(f"    {name} {sql_type}")

        return f"CREATE TABLE {table_name} (\n" + ",\n".join(column_defs) + "\n);"
