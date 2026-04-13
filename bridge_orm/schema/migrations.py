import json
import os
from datetime import datetime
from typing import Dict, List, Any, Type
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

    def generate_migration(self, description: str = "auto_migration"):
        current_snapshot = self.load_snapshot()
        new_tables = {}
        
        # Discover current models
        for table_name, model_cls in _MODEL_REGISTRY.items():
            new_tables[table_name] = model_cls.get_field_definitions()

        # Diffing logic
        sql_statements = []
        
        # 1. Detect New Tables
        for table_name, fields in new_tables.items():
            if table_name not in current_snapshot["tables"]:
                sql = self._generate_create_table(table_name, fields)
                sql_statements.append(sql)
            else:
                # 2. Detect New Columns in existing tables
                old_fields = current_snapshot["tables"][table_name]
                for field_name, field_type in fields.items():
                    if field_name not in old_fields:
                        sql_type = bridge_orm_rs.resolve_type(field_type, self.dialect)
                        sql_statements.append(f"ALTER TABLE {table_name} ADD COLUMN {field_name} {sql_type};")
                
                # 3. Detect Dropped Columns
                for field_name in old_fields:
                    if field_name not in fields:
                        sql_statements.append(f"-- WARNING: Detected dropped column '{field_name}' in table '{table_name}'.")
                        sql_statements.append(f"-- ALTER TABLE {table_name} DROP COLUMN {field_name};")

        if not sql_statements:
            print("No changes detected.")
            return

        # Generate file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}_{description}.sql"
        filepath = os.path.join(MIGRATIONS_DIR, filename)
        
        with open(filepath, "w") as f:
            f.write("\n".join(sql_statements))
        
        # Update snapshot
        current_snapshot["tables"] = new_tables
        self.save_snapshot(current_snapshot)
        
        print(f"Created migration: {filepath}")

    def _generate_create_table(self, table_name: str, fields: Dict[str, str]) -> str:
        column_defs = []
        for name, py_type in fields.items():
            sql_type = bridge_orm_rs.resolve_type(py_type, self.dialect)
            # Simple primary key logic for prototype: if field is 'id', make it PK
            if name == "id":
                sql_type += " PRIMARY KEY"
            column_defs.append(f"    {name} {sql_type}")
        
        return f"CREATE TABLE {table_name} (\n" + ",\n".join(column_defs) + "\n);"
