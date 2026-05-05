# Deployment & Features

BridgeORM uses `maturin` and `cibuildwheel` to provide a seamless installation experience across all major platforms.

## Installation

Standard installation via pip:
```bash
pip install bridge-orm
```
*Note: This installs a pre-compiled binary wheel for your platform (Linux, macOS, Windows). No Rust compiler is required on the user machine.*

## Modular Feature Gates

To keep the binary size lean and reduce the attack surface, many BridgeORM capabilities are gated behind **Cargo Features**.

| Feature | Description | Default |
| :--- | :--- | :--- |
| `postgres` | PostgreSQL driver support. | **Yes** |
| `mysql` | MySQL driver support. | No |
| `sqlite` | SQLite driver support. | No |
| `data-science` | Enables Apache Arrow zero-copy interchange. | No |
| `java-interop` | Enables JNI bindings for Java/Kotlin usage. | No |

### Installing with specific features:
```bash
pip install bridge-orm[data-science,mysql]
```

## CI/CD Pipeline

Every commit to the main branch triggers a comprehensive matrix build:

1.  **Quality Gates**: `cargo clippy`, `rustfmt`, and `mypy` checks.
2.  **Security Audit**: `cargo-deny` scans for CVEs and license compliance.
3.  **Cross-Compilation**: `cibuildwheel` builds binary wheels for:
    *   Linux (x86_64, aarch64)
    *   macOS (Intel, Apple Silicon)
    *   Windows (x86_64)
4.  **Integration Testing**: Tests are run against live PostgreSQL, MySQL, and SQLite containers in the CI environment.
