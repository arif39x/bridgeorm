# BridgeORM Studio: UI/UX Specification

This document outlines the functional requirements and visual design for **BridgeORM Studio**, a conceptual graphical interface for managing databases powered by the BridgeORM core.

---

## 1. Design Aesthetics & Visual Language
- **Theme**: High-Contrast Dark Mode ("Submarine Grey" and "Electric Blue").
- **Typography**: Monospace for all data (Fira Code) to emphasize precision.
- **Feedback**: "Alive" UI with subtle gradients and micro-animations for FFI crossings.
- **Language Indicators**: 
    - **Blue Accent**: Represents Python-side operations.
    - **Orange/Crab Accent**: Represents Rust-side execution.

---

## 2. Feature Modules

### A. The Visual Schema Explorer (ERD)
*Instead of just text, developers should see their architecture.*
- **Feature**: Auto-generated Entity Relationship Diagram.
- **Interaction**: Drag-and-drop to create relationships.
- **UI Logic**: When a user draws a line between two tables, the UI generates the Python `HasMany` or `BelongsToMany` code snippet in a side panel.
- **FFI Visualizer**: A small "Rust Bolt" icon next to each table name indicating it has been successfully whitelisted and cached in the Rust core.

### B. Migration Timeline & Dry-Run
*Manging the database lifecycle with confidence.*
- **Visual Diff**: A side-by-side comparison of `Model State` vs `Database State`.
- **Dry-Run Mode**: A "Simulate" button that runs the generated SQL migration in a temporary Rust transaction and reports success/failure without committing.
- **Timeline**: A vertical list of applied migrations with "Rollback to here" capabilities using the BridgeORM CLI backend.

### C. The Telemetry Dashboard (Real-Time)
*Visualizing the observability bridge.*
- **Flamegraphs**: Visual representation of time spent in Python (serialization) vs Rust (SQL execution).
- **Slow Query Heatmap**: A map of the database schema where tables glow red if they are frequently involved in slow queries (>100ms).
- **Live Stream**: A scrolling log of "Unified Spans" showing the exact microsecond cost of the current application traffic.

### D. The Query Laboratory
*Testing the Generic Query Engine.*
- **Interface**: A dual-pane editor.
    - **Left**: Python Query Builder syntax (`User.query().filter(...)`).
    - **Right**: The raw SQL that the Rust engine generates in real-time.
- **Explain Plan**: An integrated "Explain" button that asks Rust to fetch the database's execution plan and visualizes it as a tree structure.

### E. Data Explorer (Zero-Copy View)
*Handling millions of rows in the UI.*
- **Infinite Scroll**: Utilizes the BridgeORM `fetch_lazy()` async iterator to stream data into the grid. The UI never freezes, even when browsing a table with 10 million rows.
- **Security Audit**: A toggle that highlights any fields that are "Strictly Validated" by the Rust whitelister.

---

## 3. UX Workflows

### Scenario: Fixing a Slow Query
1. The developer notices a "Slow Query" alert in the **Telemetry Dashboard**.
2. They click the alert, which opens the **Query Laboratory** with the offending SQL pre-loaded.
3. They use the **Explain Plan** feature to identify a missing index.
4. They switch to the **Schema Explorer**, add an `@index` decorator to the model, and hit **Generate Migration**.
5. The **Visual Diff** shows the new `CREATE INDEX` statement.
6. They hit **Apply**, and the Telemetry Dashboard immediately shows the query time dropping from 500ms to 2ms.

---

## 4. Technical Implementation (Conceptual)
- **Frontend**: React + TypeScript + TailwindCSS.
- **Backend Bridge**: A specialized FastAPI server that imports the `bridge_orm` Python package and exposes its metadata via WebSockets for real-time telemetry.
- **State Management**: Redux Toolkit to handle the complex model registry state.
