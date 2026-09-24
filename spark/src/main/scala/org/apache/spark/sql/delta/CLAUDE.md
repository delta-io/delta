# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build System

Delta Lake uses SBT with the wrapper script `build/sbt` (always use this, not a system `sbt`). The project root is at `/home/timothy.wang/delta/`.

### Key Commands

```bash
build/sbt compile                    # Compile all projects
build/sbt spark/test                 # Run all spark module tests
build/sbt spark/'testOnly org.apache.spark.sql.delta.DeltaLogSuite'  # Single test suite
build/sbt spark/'testOnly org.apache.spark.sql.delta.Merge*'         # Pattern match
build/sbt kernelApi/test             # Kernel API tests
build/sbt kernelDefaults/test        # Kernel defaults tests
build/sbt sparkGroup/test            # All spark-related modules
build/sbt kernelGroup/test           # All kernel modules
build/sbt scalafmtAll                # Format Scala code
build/sbt javafmtAll                 # Format Java code (google-java-format)
build/sbt -DsparkVersion=4.0 compile # Build against Spark 4.0.x
python3 run-tests.py --group spark   # Full test runner with sharding support
```

### Versions

- Scala 2.13.17 (2.12 dropped), Spark 4.1.0 default (also supports 4.0.1), JVM target 11
- Cross-Spark shims live in `spark/src/main/scala-shims/spark-4.0/` and `spark-4.1/` (and `java-shims/`)

## Project Structure

The build has four main groups that can be tested independently:

**spark group** — The core Delta-Spark integration:
- `sparkV1` (`spark/`) — Classic DeltaLog-based read/write (V1 protocol). Not published directly.
- `sparkV1Filtered` (`spark-v1-filtered/`) — sparkV1 minus DeltaLog/Snapshot/OptimisticTransaction classes.
- `sparkV2` (`spark/v2/`) — New Kernel-based DSv2 connector in Java.
- `spark` (`spark-unified/`) — Published artifact merging sparkV1 + sparkV2. Owns all tests.
- `contribs`, `sharing`, `storage`, `storageS3DynamoDB`, `connectCommon/Client/Server`, `hudi`

**kernel group** — Standalone Java library for reading/writing Delta tables:
- `kernelApi` (`kernel/kernel-api/`) — Public APIs: `Table`, `Snapshot`, `Scan`, `Transaction`, `Engine`
- `kernelDefaults` (`kernel/kernel-defaults/`) — Default Engine implementations (Hadoop/Parquet)

**iceberg group** — UniForm Iceberg compatibility (`iceberg/`, shaded JARs)

**flink group** — Flink connector (`flink/`)

### Dependency Flow
```
storage → kernelApi → kernelDefaults → sparkV2
                                           ↓
storage → sparkV1 → sparkV1Filtered → sparkV2
                         ↓
                   spark (unified JAR)
                         ↓
           contribs, sharing, iceberg, hudi
```

## Architecture

Delta Lake implements an **ACID transaction log** as a sequence of JSON files (`_delta_log/NNNNN.json`) with periodic Parquet checkpoint files.

### Core Classes (this directory)

| File | Role |
|---|---|
| `DeltaLog.scala` | Central entry point. Manages the transaction log on disk. Mixes in `Checkpoints`, `MetadataCleanup`, `SnapshotManagement`. |
| `OptimisticTransaction.scala` | OCC transaction: reads snapshot, accumulates actions, commits atomically. Handles conflict detection via `ConflictChecker`. |
| `Snapshot.scala` | Immutable view of the table at a specific log version. Replays checkpoint + delta files. Implements `DataSkippingReader` for file pruning. |
| `DeltaConfig.scala` | Table-level config entries (e.g., `delta.logRetentionDuration`). |
| `TableFeature.scala` | Table feature protocol system (reader/writer features). |
| `DeltaErrors.scala` | Centralized error definitions. |
| `DeltaAnalysis.scala` | Spark Analyzer extension rules for Delta. |
| `sources/DeltaSQLConf.scala` | All Delta SQL configuration keys. |

### Key Subdirectories

| Directory | Contents |
|---|---|
| `actions/` | Core action types: `AddFile`, `RemoveFile`, `Metadata`, `Protocol`, `CommitInfo`, `SetTransaction`, `DomainMetadata`. These are the atomic units written to log JSON files. |
| `commands/` | DML commands: `DeleteCommand`, `UpdateCommand`, `MergeIntoCommand`, `OptimizeTableCommand`, `VacuumCommand`, `WriteIntoDelta`, `CreateDeltaTableCommand`, `ConvertToDeltaCommand`, `RestoreTableCommand`. |
| `catalog/` | `DeltaTableV2` (DSv2 table), `AbstractDeltaCatalog`, catalog resolution. |
| `sources/` | `DeltaDataSource` (DSv1 provider), `DeltaSink`/`DeltaSource` (Structured Streaming). |
| `stats/` | Statistics collection and data skipping: `StatisticsCollection`, `DataSkippingReader`, `DeltaScan`. |
| `files/` | File index implementations: `TahoeBatchFileIndex`, `TahoeLogFileIndex` ("Tahoe" = original Delta codename). |
| `coordinatedcommits/` | Coordinated Commits feature: external service coordinates versioning (e.g., Unity Catalog). |
| `deletionvectors/` | Deletion Vector support: `RoaringBitmapArray`, `StoredBitmap`, row-level deletes without rewriting files. |
| `hooks/` | Post-commit hooks: checkpoint, checksum, Iceberg/Hudi converters, auto-compact. |
| `schema/` | `SchemaUtils`, `SchemaMergingUtils`, schema evolution logic. |
| `uniform/` | UniForm (Universal Format): Iceberg + Hudi compatibility layer. |
| `clustering/` | Liquid clustering / `CLUSTER BY` support. |
| `skipping/` | Multi-dimensional clustering (Z-order, Hilbert). |

### Write Path
1. `DeltaLog.startTransaction()` → `OptimisticTransaction`
2. Command logic accumulates `Action` objects
3. `transaction.commit(actions, operation)` → writes versioned JSON atomically
4. Post-commit hooks fire (checkpoints, UniForm converters, catalog updates)

### Read Path
1. `DeltaLog.snapshot` → current `Snapshot` (replays log to compute active `AddFile` set)
2. `DataSkippingReader` prunes files using column statistics
3. `TahoeFileIndex` integrates with Spark's file source scanning

### SQL Grammar
Delta SQL extensions (DESCRIBE HISTORY, VACUUM, OPTIMIZE, RESTORE, etc.) are defined in `spark/src/main/antlr4/io/delta/sql/parser/DeltaSqlBase.g4` and compiled by sbt-antlr4.

## Code Style

- Follow [Apache Spark Scala Style Guide](https://spark.apache.org/contributing.html)
- Max line length: **100 characters**
- Scalafmt config at `.scalafmt.conf` (root); scalastyle config at `scalastyle-config.xml`
- Java code uses Google Java Format (`build/sbt javafmtAll`)
- Import order (scalafmt-enforced): `java.*`, `scala.*`, `io.delta.*`, `org.apache.spark.sql.delta.*`
- Use `JavaConverters` with `.asScala`/`.asJava` (not `JavaConversions`)
- `toUpperCase`/`toLowerCase` must use `Locale.ROOT`
- Tests must extend `SparkFunSuite` (not raw `FunSuite`)
- Don't use `spark.implicits._` — use `DeltaEncoders` or `import ...delta.implicits._`
- Don't use `spark.sessionState.newHadoopConf()` — use `deltaLog.newDeltaHadoopConf()`
- Apache 2.0 license header required on all source files

## Testing

- ScalaTest 3.2.15 (Scala), JUnit 5 (Java in sparkV2/flink), JUnit 4 (some legacy)
- Tests in `spark/src/test/scala/org/apache/spark/sql/delta/`
- `DELTA_TESTING=1` env var enables test-only table features (auto-set by build)
- Key test base classes/mixins in `spark/src/test/scala/org/apache/spark/sql/delta/test/`: `DeltaSQLCommandTest`, `DeltaTestImplicits`
- Generated test suites in `generatedsuites/` — do not edit these directly (produced by `delta-suite-generator`)
- Test parallelization via `NUM_SHARDS`, `SHARD_ID`, `TEST_PARALLELISM_COUNT` env vars
