# BridgeORM

**BridgeORM** is a cross-language ORM (Rust+Python). It is lightweight, secure by default, and immediately familiar to developers from the **Python**, **Rust**, **Go**, and **TypeScript**,**Java**,**Kotlin** ecosystems.

---

## Architecture

**Bridgeorm** uses Performance Bridge architecture Principle by spliting the ORM in 2 distinct part to maximize the **Speed** & **Developer Ergonomics**.

Python For API Provider & RUst is for Ultra Fast Performance..

## Security Mandate

So During Desigining One of the main desigining Principlle is that i try to mentain is Security...and i dont know how much i am able to apply this Principle Because I am also a human after all..

1. I avoided String Interpolation To strictly forbiddem..
2. Before any dynamic SQL identifier is executed,The **Rust Engine** forces it through a strict regular expression validator..
3. Also The FFI Boundary is wrapped in a catch_unwind block..

---

## Rules

Collaborator Must Follow This Rules:

1. **Self-Documenting Code**: Meaningful identifiers Must. If something doesn't make sense to you; then rename it to something appropriate so that its logic will inspire clarity.
2. **Single Responsibility Principle**: Each method must have only one responsibility and be of equal simplicity.
3. **D.R.Y. Principle**: Do not duplicate common functionality; instead; utilise a single reference point when using common functionality.
4. **Meaningful Identifier**: Write your identifiers as if they were spoken words. Use common sense when naming them; avoid unnecessary jargon names; and choose names with the focus of clarity.
5. **Avoid Magic Number/Strings**: Use variable constants for hard-coded numbers/strings, so their meaning is clear.
6. **Explicit Handling of Errors**: Fix the actual problem first (i.e., fix the code), then use either typed return value or exception handling to guarantee that the error is visible and cannot go unaddressed.
7. **Consistent Formatting**: Use automated tools for visual consistency across the entire codebase (if possible).
8. **Provide Explanation to your Intent**: Comment on your code to explain "why" you made those coding decisions versus only providing commentary on "what" the code is doing.
