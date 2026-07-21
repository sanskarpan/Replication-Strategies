# Architecture Decision Records

Architecture Decision Records (ADRs) capture the significant design choices made in
this project — not just *what* was decided, but *why*, and what alternatives were
considered. New contributors can read these in order to understand the reasoning behind
the system's shape.

Each record is immutable once accepted. Instead of editing a past decision, we add a
new record that supersedes it and links back.

---

## Index

| # | Title | Status |
|---|-------|--------|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](0002-replication-strategy-node-model.md) | Node interface, per-strategy node types, and FIFO-per-link transport | Accepted |
| [0003](0003-consistent-hashing-and-quorums.md) | Consistent hashing, preference lists, sloppy quorums, and region-aware quorums | Accepted |
