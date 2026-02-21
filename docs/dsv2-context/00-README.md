# DSv2 Write Path Context Docs

This folder holds local markdown documentation for implementing the **DSv2 write path** in the Delta Spark V2 connector (Kernel-based), specifically adding `SupportsWrite` to `SparkTable`.

## Purpose

- **No Claude-specific MD file** exists in this repo; this folder is the canonical context for the DSv2 write implementation.
- External design docs (Google Docs, Atlassian) require sign-in and could not be fetched automatically. Links are listed below for manual reference.
- A **PoC** ([huan233usc/delta PR #24](https://github.com/huan233usc/delta/pull/24)) implements SupportsWrite + row-level ops in a kernel-spark module; we use it for learning and reference only—we can do things differently (e.g. implement in spark/v2, different naming, scope).
- The content here is derived from: repo code (Spark DSv2, Kernel API, Delta V1 write), `kernel/USER_GUIDE.md`, public Spark/Kernel documentation, and the PoC PR.

## External Doc Links (for manual reading)

Use these in parallel with the local docs when implementing or reviewing:

| Topic | Link |
|-------|------|
| DSv2 / MCPs (doc 1) | https://docs.google.com/document/d/1POvRKJqL0ZXmFTzm0bKHRFMbb4HhPXVmzGzmKcW98qo/edit?tab=t.0 |
| DSv2 / MCPs (doc 2) | https://docs.google.com/document/d/1ZINBR5IepA0jD9Uk8SIjBO6aDvvSPDDHPhJxRHwcmyM/edit?tab=t.0 |
| Delta as DSv2 in DBR | https://databricks.atlassian.net/wiki/spaces/UN/pages/4588437509/Delta+as+a+DSv2+Data+Source+in+DBR |
| Spark DSv2 Delta connector (Kernel) | https://databricks.atlassian.net/wiki/spaces/UN/pages/3631251835/Spark+DSV2+Delta+Lake+connector+using+Delta+Kernel |
| DSv2 (doc 3) | https://docs.google.com/document/d/1Fkiee4ML2T9yWKr7Vb3U4ZKj4CwoQaawEhG-6DyzRak/edit?tab=t.rdsmiq5v2s61 |
| DSv2 (doc 4) | https://docs.google.com/document/d/1TeSXrNsLkamW3ZjjBfaZXDjmlidXKtnBe1HYLQbH-l4/edit?tab=t.9dvueh1v20mp |

## Local Doc Index

| File | Contents |
|------|----------|
| [01-dsv2-write-api.md](01-dsv2-write-api.md) | Spark DataSource V2 write API: SupportsWrite, WriteBuilder, BatchWrite, flow |
| [02-kernel-write-api.md](02-kernel-write-api.md) | Delta Kernel write flow: Transaction, TransactionBuilder, transform/commit |
| [03-spark-table-and-target.md](03-spark-table-and-target.md) | Current SparkTable (spark/v2), capabilities, and implementation target |
| [04-delta-v1-write-reference.md](04-delta-v1-write-reference.md) | How DeltaTableV2 does writes today (V1Write / InsertableRelation) |
| [05-poc-reference-huan233usc-pr24.md](05-poc-reference-huan233usc-pr24.md) | PoC reference (huan233usc/delta PR #24); learn from it, not binding |
| [CONTEXT-MAP.md](CONTEXT-MAP.md) | **Master context map** for implementing SupportsWrite in SparkTable |

Start with **CONTEXT-MAP.md** for the full picture, then drill into the numbered files as needed.
