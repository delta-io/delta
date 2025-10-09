# Delta Spark Module Structure

## Overview

The delta-spark codebase has been refactored into 5 SBT modules to support both v1 (current) and v2 (kernel-based) implementations, with **DeltaLog isolation**:

```
delta-spark-v1 (not published, full v1 with DeltaLog)
    ↓ repackage (exclude DeltaLog)
delta-spark-v1-shaded (not published, v1 without DeltaLog)
    ↓
delta-spark-v2 (not published, depends on v1-shaded)
    ↓
delta-spark-shaded (not published, optional delegation)
    ↓
delta-spark (published jar, full v1 + v2)
```

## Module Details

### 1. delta-spark-v1
- **Directory**: `spark/`
- **Published**: No
- **Content**: Production code only (no tests)
- **Description**: Current delta-spark production code
- **Key Features**:
  - All existing Delta Spark functionality
  - Antlr parser generation
  - Python file packaging

### 2. delta-spark-v1-shaded
- **Directory**: `spark-v1-shaded/` (virtual, no source files)
- **Published**: No
- **Content**: Repackaged delta-spark-v1 JAR with DeltaLog classes excluded
- **Dependencies**: delta-spark-v1
- **Description**: V1 without DeltaLog for v2 to depend on
- **Key Features**:
  - Filters out `DeltaLog`, `Snapshot`, `OptimisticTransaction` classes
  - Used to enforce v2 doesn't depend on DeltaLog at compile time
  - ~300KB smaller than full v1 (7.1M vs 7.4M)

### 3. delta-spark-v2
- **Directory**: `kernel-spark/`
- **Published**: No
- **Content**: Kernel-based Spark implementation
- **Dependencies**: **delta-spark-v1-shaded** (no DeltaLog), kernelApi, kernelDefaults
- **Description**: New kernel-based Spark connector
- **Key Features**:
  - DSv2 Catalog and Tables
  - Kernel-specific unit tests
  - **Cannot access DeltaLog at compile time** (enforced by v1-shaded dependency)

### 4. delta-spark-shaded
- **Directory**: `spark-shaded/`
- **Published**: No
- **Content**: Delegation layer
- **Dependencies**: **delta-spark-v1** (full version), delta-spark-v2
- **Description**: Contains delegation code that routes to v1 or v2
- **Key Features**:
  - DeltaCatalog (delegates to V1 or V2)
  - DeltaSparkSessionExtension (registers both)

### 5. delta-spark (final module)
- **Directory**: `spark-combined/`
- **Published**: Yes (as `delta-spark.jar`)
- **Content**: 
  - No production code (packages v1+v2+shaded)
  - All test code from `spark/src/test/`
- **Dependencies**: delta-spark-shaded, delta-spark-v1 (test utils)
- **Description**: Final published artifact combining all modules
- **Key Features**:
  - Tests can access both v1 and v2 implementations
  - Published jar contains complete v1+v2+shaded code

## File Structure

### No Files Moved!
- Production code remains in `spark/src/main/`
- Test code remains in `spark/src/test/`
- Kernel code remains in `kernel-spark/src/`

### New Directories Created
- `spark-combined/` - final combined module (v1+v2+tests)
- `spark-shaded/` - delegation code
- `spark-v1-shaded/` - virtual module (no source files, only build configuration)

## SBT Commands

```bash
# Compile individual modules
sbt delta-spark-v1/compile
sbt delta-spark-v2/compile
sbt delta-spark-shaded/compile
sbt spark/compile

# Run tests
sbt spark/test

# Publish
sbt spark/publishLocal
```

## Key Implementation Details

### Test Source Configuration
The `spark` module uses `unmanagedSourceDirectories` to point to original test locations:
```scala
Test / unmanagedSourceDirectories ++= Seq(
  baseDirectory.value.getParentFile / "spark" / "src" / "test" / "scala",
  baseDirectory.value.getParentFile / "spark" / "src" / "test" / "java"
)
```

### Package Assembly
The final `spark` module packages all classes:
```scala
Compile / packageBin / mappings := {
  val v1 = (`delta-spark-v1` / Compile / packageBin / mappings).value
  val v2 = (`delta-spark-v2` / Compile / packageBin / mappings).value
  val shaded = (`delta-spark-shaded` / Compile / packageBin / mappings).value
  v1 ++ v2 ++ shaded
}
```

## Benefits

1. **Modular**: Clear separation between v1, v2, and delegation layers
2. **No File Movement**: All code stays in original locations
3. **Backward Compatible**: Final jar contains everything
4. **Testable**: Tests can verify both v1 and v2 implementations
5. **Not Published**: Internal modules (v1, v2, shaded) aren't published
6. **Clean Dependencies**: Avoids circular dependencies

## Migration Notes

### Dependency Updates
Modules that previously depended on `spark` should now depend on:
- `delta-spark-v1` - if only v1 functionality needed
- `delta-spark-shaded` - if both v1 and v2 needed
- `spark` - if test utilities needed

### Updated Dependencies
- `kernelDefaults` → depends on `delta-spark-v1 % "test->test"`
- `goldenTables` → depends on `delta-spark-v1 % "test"`

