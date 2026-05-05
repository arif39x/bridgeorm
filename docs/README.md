# BridgeORM Documentation

Welcome to the official documentation for **BridgeORM**, a high-performance, cross-language ORM combining the expressiveness of Python with the safety and speed of Rust.

## Table of Contents

1.  **[Architecture Overview](architecture.md)**
    *   Design Philosophy
    *   The "Performance Bridge" Pattern
    *   FFI Boundary & Safety (`ffi_guard!`)
2.  **[Querying & Relations](query_builder.md)**
    *   The Fluent Query Builder
    *   Eager Loading Strategies (`prefetch_related`)
    *   Dialect-Agnostic SQL Generation
3.  **[Concurrency & Data Integrity](concurrency.md)**
    *   Concurrent Shared Identity Map
    *   Optimistic Concurrency Control (OCC)
    *   Async Runtime Synchronization (Tokio/Asyncio)
4.  **[Deployment & Features](deployment.md)**
    *   Binary Wheel Installation
    *   Modular Feature Gates (`data-science`, `java-interop`)
    *   CI/CD Pipeline & Build Matrix
5.  **[Security & Standards](security.md)**
    *   SQL Injection Prevention
    *   Supply Chain Security (`cargo-deny`)
    *   Panic Recovery & Telemetry

---

## Core Mandates

BridgeORM is built on a set of non-negotiable engineering rules:

*   **FFI Safety**: Every cross-boundary call is wrapped in `ffi_guard!` to prevent process aborts.
*   **Dialect Agnosticism**: All SQL is emitted via Rust traits — no raw string concatenation.
*   **Zero-Copy Interchange**: Large datasets move between Rust and Python via Apache Arrow (optional).
*   **Telemetry First**: Every database operation carries a `tracing::span`. If it isn't measured, it isn't merged.
