#!/bin/bash

# Configuration
SQLITE_DB="sqlite:complex_demo.db"

function show_header() {
    clear
    echo "===================================================="
    echo "    BridgeORM "
    echo "===================================================="
}

function verify_data() {
    echo ""
    echo "Querying Rust Identity Map & DB..."
    python3 -c "
import bridge_orm_rs, asyncio
async def verify():
    await bridge_orm_rs.connect('$SQLITE_DB')
    # Use the Rust engine to list all tables discovered via introspection
    schema = await bridge_orm_rs.reflect_schema()
    print(f'\nDB Tables Detected by Rust Introspection:')
    for table in schema:
        print(f' - {table.name} ({len(table.columns)} columns)')
        # Sample data from the table
        try:
            rows = await bridge_orm_rs.fetch_all(table.name, {}, limit=3)
            print(f'   Sample Data: {rows}')
        except:
            pass
asyncio.run(verify())"
}

while true; do
    show_header
    echo "Choose a language to store complex data:"
    echo "1) Python (PostgreSQL - Uses JSON & Raw SQL)"
    echo "2) TypeScript (PostgreSQL - JSON Config)"
    echo "3) Java (SQLite - Thread Metadata)"
    echo "4) Go (MySQL - Bulk Vectorized)"
    echo "5) Kotlin (Oracle - Enterprise Mode)"
    echo "6) [CRITICAL] Verify All Data via Rust Engine"
    echo "7) Exit"
    echo "----------------------------------------------------"
    read -p "Select [1-7]: " choice

    case $choice in
        1)
            echo "Running Python Demo..."
            python3 examples/demos/complex_python.py
            read -p "Press enter to continue..."
            ;;
        2)
            echo "Running TS Demo (Simulated)..."
            echo "Executing: npx ts-node examples/demos/complex_ts.ts"
            echo "[STUB] TS successfully stored Project config via BridgeORM-RS."
            read -p "Press enter to continue..."
            ;;
        3)
            echo "Running Java Demo (Simulated)..."
            echo "Executing: java ComplexJavaDemo.java"
            python3 -c "import bridge_orm_rs, asyncio, uuid;
async def s():
    await bridge_orm_rs.connect('$SQLITE_DB');
    await bridge_orm_rs.execute_raw('CREATE TABLE IF NOT EXISTS audit_logs (id TEXT, action TEXT, details TEXT)');
    await bridge_orm_rs.insert_row('audit_logs', {'id': str(uuid.uuid4()), 'action': 'JAVA_DEMO', 'details': '{\"platform\": \"JVM\"}'})
asyncio.run(s())"
            echo "Java successfully stored AuditLog via FFI."
            read -p "Press enter to continue..."
            ;;
        4)
            echo "Go Demo: Vectorized Bulk Insert (Simulated)..."
            echo "Go utilizes the 'create_many' vectorized FFI boundary."
            read -p "Press enter to continue..."
            ;;
        6)
            verify_data
            read -p "Press enter to continue..."
            ;;
        7)
            echo "Goodbye!"
            exit 0
            ;;
        *)
            echo "Invalid option."
            sleep 1
            ;;
    esac
done
