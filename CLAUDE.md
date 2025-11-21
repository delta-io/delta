# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Delta Lake is an open-source storage framework providing ACID transactions and scalable metadata handling for data lakes. This repository contains multiple components:

- **Delta Spark (spark/)**: The core Spark connector for Delta Lake (v1 implementation using DeltaLog)
- **Delta Kernel (kernel/)**: A protocol-agnostic library for building Delta connectors
  - `kernel-api/`: Public APIs for Table and Engine interfaces
  - `kernel-defaults/`: Default Engine implementation using Hadoop libraries
  - `kernel-benchmarks/`: Performance benchmarking suite
- **Delta Kernel Spark (kernel-spark/)**: Spark connector built using Delta Kernel (v2 pure DSv2)
- **Spark Unified (spark-unified/)**: Final published `delta-spark` JAR combining v1 and v2
- **Unity (unity/)**: Unity Catalog integration with catalog-managed commit coordination
- **Storage (storage/, storage-s3-dynamodb/)**: Storage layer abstractions and S3 DynamoDB support
- **Connectors (connectors/)**: Various Delta Lake connectors (Flink, standalone, Hudi, Iceberg)
- **Python (python/)**: Python bindings for Delta Lake

## Build System

The project uses **SBT** (Scala Build Tool) with Java 11 as the target JVM.

### Common Build Commands

```bash
# Build the project
build/sbt compile

# Generate artifacts
build/sbt package

# Run all tests
build/sbt test

# Run tests for a specific project group
build/sbt sparkGroup/test
build/sbt kernelGroup/test

# Run a single test suite
build/sbt spark/'testOnly org.apache.spark.sql.delta.optimize.OptimizeCompactionSQLSuite'

# Run a specific test within a suite
build/sbt spark/'testOnly *.OptimizeCompactionSQLSuite -- -z "optimize command: on partitioned table - all partitions"'

# Run tests with coverage
build/sbt coverage test coverageAggregate coverageOff

# Run with different Spark version
build/sbt -DsparkVersion=master compile
build/sbt -DsparkVersion=3.5.7 test
```

### Code Formatting

**Java code** must follow Google Java Style:
```bash
# Check Java formatting
build/sbt javafmtCheckAll

# Fix Java formatting
build/sbt javafmtAll
```

**Scala code** uses Scalafmt (config in `.scalafmt.conf`):
```bash
# Check Scala formatting
build/sbt scalafmtCheckAll

# Fix Scala formatting
build/sbt scalafmtAll

# Check Scala style
build/sbt scalastyle
```

### Python Tests

```bash
# Setup conda environment
conda env create --name delta_python_tests --file=<absolute_path>/python/environment.yml
conda activate delta_python_tests

# Run Python tests
python3 <delta-root>/python/run-tests.py
```

### Test Groups

The codebase is organized into test groups that can be run independently:
- `spark`: Spark-related tests
- `kernel`: Delta Kernel tests
- `iceberg`: Iceberg compatibility tests
- `spark-python`: Python integration tests

Use `build/sbt <group>Group/test` to run a specific group.

## Architecture

### Delta Kernel Architecture

Delta Kernel provides two sets of APIs:

1. **Table APIs** (`io.delta.kernel.*`): High-level interfaces for reading/writing Delta tables
   - `Table`: Entry point for accessing Delta tables
   - `Snapshot`: Point-in-time view of a Delta table
   - `Scan`: API for reading data with filters and column pruning
   - `Transaction`: API for writing data to Delta tables

2. **Engine APIs** (`io.delta.kernel.engine.*`): Pluggable interfaces for compute-intensive operations
   - `Engine`: Main interface for connector-specific optimizations
   - `ParquetHandler`: Parquet file reading/writing
   - `JsonHandler`: JSON processing for Delta logs
   - `FileSystemClient`: File system operations
   - Default implementations provided in `kernel-defaults` module

### V1 vs V2 Architecture

Delta Lake provides two Spark connector implementations that coexist in the published JAR:

#### V1 Implementation (DeltaLog-based)
Located in `spark/`, this is the traditional connector using:
- **DeltaLog**: Core entry point for transaction log access and management
- **Snapshot**: In-memory representation of table state at a version
- **OptimisticTransaction**: Handles ACID transactions with conflict detection
- **DeltaTable**: Public Scala/Java/Python API
- Hybrid DataSource V1 and V2 APIs for compatibility

**Core Components**:
- `DeltaLog.scala`: Transaction log management and caching
- `Snapshot.scala`: Table state with file lists and metadata
- `OptimisticTransaction.scala`: Write path with conflict resolution
- `commands/`: Command implementations (WriteIntoDelta, MergeIntoCommand, etc.)
- `catalog/`: AbstractDeltaCatalog with table resolution

#### V2 Implementation (Kernel-based)
Located in `kernel-spark/`, this is a pure DataSource V2 connector using Delta Kernel:
- Uses Delta Kernel APIs (`io.delta.kernel.*`) for protocol handling
- Depends on `sparkV1Filtered` for shared utilities (excluding DeltaLog)
- Designed as a reference implementation for building Delta connectors
- Leverages Spark's ParquetFileFormat for actual data reads

**Architecture**:
```
Spark Driver → Delta Kernel Connector → Delta Kernel API
                                           ↓
                                   (reads Delta logs)
                                           ↓
Spark Engine ← file splits ← Delta Kernel returns scan files
    ↓
(Parquet scans using Spark's built-in reader)
```

#### Unified Connector (spark-unified/)
The published `delta-spark` JAR combines both implementations:
- Provides single entry points: `DeltaCatalog` and `DeltaSparkSessionExtension`
- V1 handles most operations (writes, commands)
- V2 provides alternative read path via DSv2
- No duplicate classes - sparkV1Filtered ensures clean merge

### Module Dependencies and Build Flow

The repository uses a sophisticated multi-module build structure:

```
storage (base storage abstraction)
    ↓
kernelApi (kernel/kernel-api) → kernelDefaults (kernel/kernel-defaults)
    ↓                                      ↓
    |                                sparkV2 (kernel-spark/)
    |                                      ↓
    └──────────────────────────────> unity (unity/)

sparkV1 (spark/) ──────> sparkV1Filtered (spark-v1-filtered/)
                                ↓
                          sparkV2 (kernel-spark/)
                                ↓
                          spark (spark-unified/) [PUBLISHED JAR]
```

**Key Points:**
- `sparkV1` contains the complete v1 implementation with DeltaLog, Snapshot, OptimisticTransaction
- `sparkV1Filtered` is a filtered version excluding DeltaLog classes (to avoid conflicts)
- `sparkV2` (kernel-spark) depends on sparkV1Filtered for shared utilities
- `spark` (spark-unified) is the final published artifact merging all three modules
- `unity` provides Unity Catalog integration for catalog-managed tables (CCv2)

**Published vs Internal Modules:**
- Published: `delta-spark` (spark-unified), `delta-kernel-api`, `delta-kernel-defaults`, `delta-unity`
- Internal (not published): `delta-spark-v1`, `delta-spark-v1-filtered`, `delta-spark-v2`

### Unity Catalog Integration (unity/)

The Unity module provides integration with Unity Catalog for catalog-managed tables:

**Key Components**:
- `UCCatalogManagedCommitter`: Implements coordinated commits version 2 (CCv2)
- `UCCatalogManagedClient`: Client for Unity Catalog commit coordination
- Catalog-managed tables use feature flags in table properties:
  - `delta.feature.catalogManaged = supported`
  - `delta.feature.catalogOwned-preview = supported`

**Detection**: Use `CatalogTableUtils` (in kernel-spark) to check if a table is catalog-managed:
- `isCatalogManaged(CatalogTable)`: Checks for catalog management feature
- `isUnityCatalogManagedTable(CatalogTable)`: Verifies UC-specific table ID

### Protocol Implementation

The Delta transaction log protocol is defined in `PROTOCOL.md`. Key concepts:

- **Actions**: Metadata changes recorded in JSON log entries (AddFile, RemoveFile, Protocol, etc.)
- **Checkpoints**: Parquet files that snapshot the table state for fast access
- **Table Features**: Protocol capabilities (column mapping, deletion vectors, row tracking, etc.)
  - Defined in `TableFeature.scala` with reader/writer version requirements
  - Features can be legacy (version-based) or name-based
- **Concurrency Control**: Optimistic concurrency with conflict detection and retry logic

## Key File Locations

### Spark V1 Implementation (spark/)
- Core Delta classes: `spark/src/main/scala/org/apache/spark/sql/delta/`
  - `DeltaLog.scala`: Transaction log management
  - `Snapshot.scala`: Table state snapshots
  - `OptimisticTransaction.scala`: ACID transaction handling
  - `commands/`: DML operations (MERGE, UPDATE, DELETE, OPTIMIZE, VACUUM)
  - `catalog/`: Catalog integration (AbstractDeltaCatalog)
  - `actions/`: Protocol actions (AddFile, RemoveFile, Metadata, etc.)

### Delta Kernel (kernel/)
- API definitions: `kernel/kernel-api/src/main/java/io/delta/kernel/`
  - `Table.java`, `Snapshot.java`, `Scan.java`, `Transaction.java`
  - `engine/Engine.java`: Pluggable engine interface
- Default implementations: `kernel/kernel-defaults/src/main/java/io/delta/kernel/defaults/`
  - `DefaultEngine.java`: Hadoop-based Engine
  - Parquet/JSON handlers, file system client

### Kernel-Spark Connector (kernel-spark/)
- Connector code: `kernel-spark/src/main/java/io/delta/kernel/spark/`
  - `read/`: DSv2 read support
  - `snapshot/`: Snapshot management
  - `catalog/`: Catalog integration for kernel
  - `utils/`: Bridge utilities between Kernel and Spark
    - `CatalogTableUtils.java`: Unity Catalog metadata helpers
    - `SchemaUtils.java`: Schema conversion utilities

### Spark Unified (spark-unified/)
- Entry points: `spark-unified/src/main/`
  - `java/.../DeltaCatalog.java`: Unified catalog (extends AbstractDeltaCatalog)
  - `scala/.../DeltaSparkSessionExtension.scala`: Session extension

### Unity Catalog Integration (unity/)
- UC support: `unity/src/main/java/io/delta/unity/`
  - `UCCatalogManagedCommitter.java`: Catalog-managed commits (CCv2)
  - `UCCatalogManagedClient.java`: Unity Catalog client

### Tests
- Tests mirror source structure: `<module>/src/test/{scala,java}/`
- Shared test utilities: `<module>/src/test/scala/.../testUtils/`

## Development Workflow

### IntelliJ Setup

1. Import project: `File` > `New Project` > `Project from Existing Sources` > select delta directory
2. Choose `sbt` as external model, use Java 11 JDK
3. Run `build/sbt clean package` to generate necessary files
4. If you see parser errors, run `build/sbt clean compile` and refresh IntelliJ
5. For source folder issues: `File` > `Project Structure` > `Modules` > remove target folders

### Contributing

- Sign commits with `git commit -s` (Developer Certificate of Origin required)
- Follow Apache Spark Scala Style Guide
- Major features (>100 LOC) require discussion via GitHub issue first
- Run formatting checks before committing: `build/sbt javafmtAll scalafmtAll`

## Testing Strategy

### Test Naming Conventions

- Scala tests: `*Suite.scala` (e.g., `DeltaLogSuite`, `OptimizeCompactionSQLSuite`)
- Java tests: `*Test.java` (e.g., `CatalogTableUtilsTest`)

### Running Tests Efficiently

When working on a specific module:
```bash
# Test only kernel module
build/sbt kernel-api/test

# Test specific package within spark
build/sbt spark/'testOnly org.apache.spark.sql.delta.commands.*'

# Run with increased logging
build/sbt 'set logLevel := Level.Debug' spark/test
```

### Test Configuration

Tests are configured for local execution with:
- Spark UI disabled
- Reduced shuffle partitions (5)
- Limited memory (1GB)
- Small log cache sizes

See `build.sbt` `commonSettings` section for full test JVM options.

## Protocol and Table Features

When working on protocol changes or table features:

1. Review `PROTOCOL.md` for specification details
2. Table features are defined in `spark/src/main/scala/org/apache/spark/sql/delta/TableFeature.scala`
3. Protocol version changes require updating reader/writer version requirements
4. Add feature documentation to `protocol_rfcs/` for major changes

## Common Pitfalls

### Java Version
- Must use Java 11 for building
- Set `JAVA_HOME` appropriately: `export JAVA_HOME=$(/usr/libexec/java_home -v 11)` (macOS)

### SBT Commands
- Always use `build/sbt` not bare `sbt` (uses project-specific version)
- Use single quotes for commands with spaces: `build/sbt 'testOnly *.Suite'`

### Cross-Scala Builds
- Default Scala version is 2.13.16
- Use `+ <command>` to run for all Scala versions: `build/sbt "+ compile"`
- Use `++ 2.13.16` to set specific version: `build/sbt "++ 2.13.16" compile`

### Test Parallelization
- Tests run in parallel by default
- Some tests may be flaky when run in parallel
- Use `Test / parallelExecution := false` in specific project if needed

## Git Workflow with Stacked Branches

This project uses **git stack** for managing stacked branches. Common commands:

```bash
# Visualize stack
git stack ls

# Navigate between branches
git stack jump BRANCH

# Create child branch from current
git stack create BRANCH

# Daily workflow
<edit code>
git stack commit              # or regular git commit
git stack sync                # rebase children onto updated parent
git stack push origin         # push branches + manage PRs
git stack push origin --publish  # mark all PRs ready for review

# Branch management
git stack remove BRANCH       # drop a branch from stack
git stack clear               # forget stored PR links
git stack abort               # recover from failed sync/rebase
```

**Important**: Never add "Co-Authored-By: Claude" to commits.

## CCv2 Project Context

**Current Focus**: Implementing Coordinated Commits V2 (CCv2) support in Delta Kernel's DSv2 Connector for Unity Catalog-managed tables.

### Architecture Overview

CCv2 enables Unity Catalog to manage Delta table commits through coordinated commit coordination:

1. **Catalog-managed tables** have feature flags in table properties:
   - `delta.feature.catalogManaged = supported`
   - `delta.feature.catalogOwned-preview = supported`
   - `ucTableId` identifier for Unity Catalog

2. **Components**:
   - `UCCatalogManagedClient` (unity/): Loads snapshots for UC-managed tables
   - `UCCatalogManagedCommitter` (unity/): Handles commits via UC
   - `CatalogTableUtils` (kernel-spark/): Detection utilities for catalog management
   - `UCClientFactory`: Creates UC clients (currently token-based)

3. **Current Work**: Extending CCv2 support with additional features and integration tests.

### Completed Work

- **PR #5477**: Added utilities for detecting CCv2 tables
  - `CatalogTableUtils.java`: Methods to check if table is catalog-managed
  - `ScalaUtils.java`: Scala-Java conversion utilities
  - Test utilities in kernel-spark module

- **PR #5520**: Implemented snapshot manager infrastructure for UC-managed tables
  - `CatalogManagedSnapshotManager`: DeltaSnapshotManager implementation for UC tables
  - `DeltaSnapshotManagerFactory`: Factory pattern for creating appropriate snapshot managers
  - `SparkTable` integration: Automatically uses correct manager based on table type
  - Added `unity` as dependency to `kernel-spark` module
  - Unit tests for null validation and factory behavior

### Key Files for CCv2

- `unity/src/main/java/io/delta/unity/`
  - `UCCatalogManagedClient.java`: Client for UC-managed Delta tables
  - `UCCatalogManagedCommitter.java`: Commit coordination logic
  - `adapters/`: Protocol and metadata adapters between Kernel and storage layers
- `spark/src/main/scala/.../coordinatedcommits/UCCommitCoordinatorBuilder.scala`
  - `UCClientFactory` trait and `UCTokenBasedRestClientFactory`
- `kernel-spark/src/main/java/io/delta/kernel/spark/`
  - `utils/CatalogTableUtils.java`: Detection utilities for catalog-managed tables
  - `snapshot/CatalogManagedSnapshotManager.java`: Snapshot manager for UC tables
  - `snapshot/DeltaSnapshotManagerFactory.java`: Factory for manager selection
  - `table/SparkTable.java`: Uses factory to create appropriate snapshot manager

### Known Limitations and Future Work

**Resource Lifecycle Management (TODO for next PR):**
- `CatalogManagedSnapshotManager` implements `AutoCloseable` but `DeltaSnapshotManager` interface doesn't
- `SparkTable` doesn't implement close() or manage snapshot manager resources
- UC client connections may leak in long-running Spark sessions
- Options to consider:
  - Make SparkTable implement AutoCloseable
  - Add AutoCloseable to DeltaSnapshotManager interface with default no-op
  - Use shutdown hooks as safety net (not recommended as primary solution)

**Unsupported Operations (Architectural Limitations):**
- `getActiveCommitAtTime()`: Throws `UnsupportedOperationException`
  - Requires filesystem-based commit history (not available for UC tables)
  - Unity Catalog coordinates commits differently
- `getTableChanges()`: Throws `UnsupportedOperationException`
  - Needs UC API support for commit range functionality

**Performance Optimizations Needed:**
- `checkVersionExists()` loads full snapshot just to check version existence
  - Very inefficient for tables with large file lists
  - TODO: Add lightweight version checking API to `UCCatalogManagedClient`
- No connection pooling for UC clients (new HTTP client per manager instance)
- No snapshot caching strategy (loads fresh from UC every time)

**Module Dependencies:**
- Added `unity` as dependency to `kernel-spark` (sparkV2)
- This makes kernel-spark Unity-aware (appropriate for UC-managed tables)
- Creates tight coupling to Unity implementation
- Consider if UC support should be pluggable via SPI pattern in the future

## Working with Different Modules

### When to modify which module:

**spark/** - Modify when:
- Adding/fixing V1 connector features (DeltaLog, OptimisticTransaction)
- Implementing new commands (DML operations)
- Changing Spark catalog integration
- Working on most production Spark features

**kernel/kernel-api/** - Modify when:
- Adding new Table or Engine API methods
- Changing public Kernel interfaces
- This requires careful API compatibility considerations

**kernel/kernel-defaults/** - Modify when:
- Improving default Engine implementations
- Fixing Parquet/JSON handling in default engine
- Adding new default utilities

**kernel-spark/** - Modify when:
- Working on V2 DSv2 connector
- Adding Kernel-specific Spark integration
- Bridging Kernel and Spark types (utils/)
- Testing Kernel with Spark

**spark-unified/** - Rarely modified:
- Only for entry point changes
- Usually just extends classes from spark/

**unity/** - Modify when:
- Working on Unity Catalog integration
- Implementing catalog-managed commit coordination
- Adding UC-specific features

### Module Testing Strategy

When working across modules:
1. Test individual module first: `build/sbt <module>/test`
2. Test the group: `build/sbt <module>Group/test`
3. Test integration in spark-unified if needed
4. Remember that kernel-spark tests may use sparkV1Filtered classes

**For CCv2/kernel-spark work**, use:
```bash
build/sbt -DsparkVersion=master "++ 2.13.16" clean sparkV2/test
```
Note: Test output can be very large, monitor carefully.

### Testing Unity Catalog Managed Tables

**Unit Test Limitations:**
Testing UC-managed table logic has inherent limitations in unit tests:
- UC table validation requires real `SparkSession` with catalog configuration
- Cannot mock `SparkSession` effectively for UC catalog access
- Constructor null checks happen before business logic validation
- Factory pattern requires non-null SparkSession for consistent API

**What CAN be unit tested:**
- ✅ Null parameter validation (requireNonNull checks)
- ✅ Factory creation with mock catalog tables
- ✅ Interface contract validation
- ✅ Exception type verification

**What REQUIRES integration tests:**
- ❌ UC table detection and validation logic
- ❌ Snapshot loading from Unity Catalog
- ❌ UC client creation and lifecycle
- ❌ Manager selection based on table type
- ❌ End-to-end flows with real UC tables

**Integration Test Requirements:**
1. Real SparkSession with Unity Catalog configured
2. `spark.sql.catalog.<name>.uri` and `.token` properties set
3. UC tables with proper feature flags (`delta.feature.catalogManaged`)
4. Unity Catalog endpoint available for testing

**Pattern to Follow:**
- Unit tests: Focus on null validation and basic contract verification
- Document integration test needs clearly in comments
- Avoid tests that require SparkSession but pass null (they'll fail on null checks)
- Accept that some validations can only be tested with real UC setup

**Example from CatalogManagedSnapshotManagerTest:**
```java
// ✅ Good unit test - validates null check
@Test
void testConstructor_NullSpark_ThrowsException() {
  assertThrows(NullPointerException.class, ...);
}

// ❌ Cannot be unit tested - requires real SparkSession
// Documented as needing integration test instead
// - testConstructor_NonUCTable_ThrowsException
// - testConstructor_ValidUCTable_Success
```

## Development Best Practices

### Before Adding New Code

**Always check if similar code exists** before creating new utilities or implementations:
- Search the codebase for similar functionality
- Look for existing patterns in related modules
- Reuse existing utilities when possible

Example: Before creating new UC configuration extraction utilities, check if `spark/` or `unity/` already has them.

### Naming and Documentation

- Prefix all Delta-specific APIs with "Delta"
- Use precise terminology: "commit" vs "version", "DAO" vs "model"
- Keep config keys aligned with canonical definitions
- Add inline comments explaining:
  - Onboarding states
  - Why helper fields exist
  - Prerequisite PRs or follow-up work
- Document adapter/wrapper classes explaining their purpose
- Delete unused imports, constructors, helpers immediately

### Configuration

- Only configure what's needed - avoid unused properties
- Don't duplicate settings that are set centrally
- Maintain single naming scheme: spec → SDK → server → connector
- Centralize config constants and reuse everywhere
- Document meaning, units, and defaults at declaration site

### Code Structure

- Avoid one-off interfaces
- Collapse duplicate setters
- Encapsulate internal setup inside constructors
- Keep call sites minimal
- Prefer composition over inheritance

### Testing Requirements

For every behavioral change:
- Add regression tests covering both new and existing behavior
- Test success and failure paths
- Test with non-default config values (ask: "would test pass if this config were ignored?")
- Parameterize across error codes, statuses, exceptions
- Exercise public APIs rather than internal repositories
- Clean up shared resources after tests

### Retry and Resilience

When adding retry logic:
- Centralize in shared transport layers
- Classify retryable failures by inspecting full cause chain
- Record attempt counts and elapsed time
- Keep loops simple: detect, guard, backoff, rethrow
- Use injected clock abstraction for time handling

### Error Handling

- Use validation helpers consistently (`ValidationUtils.checkArgument`, Guava `Preconditions`)
- Enforce strict bounds (no zero-delay retries, no negative attempts)
- Surface follow-up work via issues, not by expanding current PR

## Explaining Codebase Concepts

When explaining any complex concept:
1. Search the repository first
2. Include a simple diagram or conversational flow
3. Use "When X happens, respond with Y; if Z occurs, do A otherwise B" patterns
4. Aim for ELI10 (Explain Like I'm 10) clarity
5. Don't just repeat the question
