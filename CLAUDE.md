# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build System

Delta Lake uses SBT via the wrapper script `build/sbt` (always use this, not a system `sbt`).

```bash
build/sbt compile                    # Compile all projects
build/sbt spark/compile              # Compile spark module only
build/sbt spark/test                 # Run all spark module tests
build/sbt spark/'testOnly org.apache.spark.sql.delta.DeltaLogSuite'           # Single test suite
build/sbt spark/'testOnly *.OptimizeCompactionSQLSuite -- -z "test name"'     # Single test by name
build/sbt sparkUnityCatalog/test     # Run UC integration tests (starts local UC server)
build/sbt 'sparkUnityCatalog/testOnly io.sparkuctest.UCDeltaTableDMLTest'     # Single UC test class
build/sbt scalafmtAll                # Format Scala code
build/sbt javafmtAll                 # Format Java code
```

Versions: Scala 2.13.17, Spark 4.1.0 default (also 4.0.1), JVM target 11.

## Detailed Module Documentation

- **`spark/src/main/scala/org/apache/spark/sql/delta/CLAUDE.md`** — Core spark module architecture, class reference, code style rules, testing details
- **`spark/unitycatalog/src/test/java/io/sparkuctest/CLAUDE.md`** — UC integration test setup, test patterns, remote UC testing

## Project Structure

Four module groups:

| Group | Key Modules | Test Command |
|-------|-------------|--------------|
| **spark** | `sparkV1` (spark/), `sparkV2` (spark/v2/), `spark` (spark-unified/) | `build/sbt sparkGroup/test` |
| **kernel** | `kernelApi` (kernel/kernel-api/), `kernelDefaults` (kernel/kernel-defaults/) | `build/sbt kernelGroup/test` |
| **iceberg** | `iceberg` (iceberg/) | `build/sbt icebergGroup/test` |
| **flink** | `flink` (connectors/flink/) | `build/sbt flinkGroup/test` |

The `spark` (spark-unified/) project is the published Maven artifact `delta-spark`. It merges classes from `sparkV1` and `sparkV2` and owns all tests. When running `build/sbt spark/test`, you're testing the unified module.

## Unity Catalog Integration Architecture

The Delta-UC integration uses a **delegation chain with dynamic class loading**. This is the most architecturally complex cross-project boundary.

### Catalog Plugin Chain

```
Spark SQL query
  ↓
UCSingleCatalog                          (io.unitycatalog.spark)
  ├── Loads DeltaCatalog via reflection   (Class.forName at init)
  ├── Creates UCProxy                     (internal UC client wrapper)
  └── Sets DeltaCatalog.delegate = UCProxy
        ↓
DeltaCatalog → AbstractDeltaCatalog      (org.apache.spark.sql.delta.catalog)
  ├── extends DelegatingCatalogExtension
  ├── Detects UC via delegate classname   (reflection: startsWith "io.unitycatalog.")
  └── Delegates namespace/table ops to UCProxy
        ↓
UCProxy → UC Server APIs                 (TablesApi, TemporaryCredentialsApi)
```

Configured via: `spark.sql.catalog.<name>=io.unitycatalog.spark.UCSingleCatalog`

### Key Integration Files

**Unity Catalog side** (`~/unitycatalog/connectors/spark/`):
- `UCSingleCatalog.scala` — Catalog plugin entry point. Composes DeltaCatalog + UCProxy.
- `UCProxy` (inner) — `TableCatalog` + `SupportsNamespaces` that talks to UC server.

**Delta side** (`spark/src/main/scala/org/apache/spark/sql/delta/`):
- `catalog/AbstractDeltaCatalog.scala` — Base catalog with UC-aware managed table handling and server-side planning.
- `catalog/DeltaTableV2.scala` — DSv2 table wrapper returned by catalog.loadTable().
- `coordinatedcommits/` — `UCCommitCoordinatorBuilder` detects UC catalogs and creates `UCCommitCoordinatorClient` for distributed commit coordination.

### Table Operations Flow

**CREATE TABLE (managed)**:
1. `UCSingleCatalog.createTable()` validates `delta.feature.catalogManaged=supported`
2. Calls UC `TablesApi.createStagingTable()` → gets staging location + table ID
3. Calls UC `TemporaryCredentialsApi` → gets vended credentials
4. Passes location + credentials + table ID as table properties to `DeltaCatalog.createTable()`
5. `AbstractDeltaCatalog` creates the Delta log at the staged location

**LOAD TABLE (read path)**:
1. `UCProxy.loadTable()` calls UC `TablesApi.getTable()` for metadata
2. Requests temporary credentials (READ_WRITE, falls back to READ)
3. Returns `V1Table` with credential-enriched properties
4. `AbstractDeltaCatalog.loadTable()` wraps as `DeltaTableV2`
5. If credentials are null and server-side planning is enabled, Delta delegates scan planning to UC

**Credential propagation**: UC sets properties in two formats — with and without `option.` prefix — because Delta reads credentials from both table properties and Hive metastore storage properties.

### Testing the Integration

The `sparkUnityCatalog` module (`spark/unitycatalog/`) is a test-only Java module:
- Starts an embedded UC server (`UnityCatalogSupport`)
- `@TestAllTableTypes` generates EXTERNAL and MANAGED test variants
- `S3CredentialFileSystem` fakes S3 for local testing, asserting credential propagation
- See `spark/unitycatalog/src/test/java/io/sparkuctest/CLAUDE.md` for full details
