# Delta Spark Unified Module

This module contains the final, published `delta-spark` JAR that unifies both:
- **V1 (hybrid DSv1 and DSv2)**: The traditional Delta Lake connector using `DeltaLog` for Delta support
- **V2 (Pure DSv2)**: The new Kernel-backed connector.

## Architecture

The unified module provides single entry points for both V1 and V2:
- `DeltaCatalog`: Extends `AbstractDeltaCatalog` from the `spark` module
- `DeltaSparkSessionExtension`: Extends `AbstractDeltaSparkSessionExtension` from the `spark` module

## Module Structure

```
spark-unified/           (This module - final published artifact)
  ├── src/main/java/
  │   └── org.apache.spark.sql.delta.catalog.DeltaCatalog.java
  └── src/main/scala/
      └── io.delta.sql.DeltaSparkSessionExtension.scala

spark/                   (sparkV1 - V1 implementation)
  ├── Core Delta Lake classes with DeltaLog
  ├── AbstractDeltaCatalog, AbstractDeltaSparkSessionExtension
  └── v2/                (sparkV2 - V2 implementation)
      └── Kernel-backed DSv2 connector
```

## How It Works

1. **sparkV1** (`spark/`): Contains production code for the V1 connector using DeltaLog
2. **sparkV1Filtered** (`spark-v1-shaded/`): Filtered version of V1 excluding DeltaLog, Snapshot, OptimisticTransaction, and actions.scala
3. **sparkV2** (`spark/v2/`): Kernel-backed V2 connector that depends on sparkV1Filtered
4. **spark** (this module): Final JAR that merges V1 + V2 classes

The final JAR includes:
- All classes from sparkV1, sparkV2, and storage
- Python files
- No internal module dependencies in the published POM

## Internal vs Published Modules

**Internal modules** (not published to Maven):
- `delta-spark-v1`
- `delta-spark-v1-filtered`
- `delta-spark-v2`

**Published module**:
- `delta-spark` (this module) - contains merged classes from all internal modules

## Build

The module automatically:
1. Merges classes from V1, V2, and storage modules
2. Detects duplicate classes (fails build if found)
3. Filters internal modules from POM dependencies
4. Exports as JAR

## Testing

Tests are located in `spark/src/test/` and run against the combined JAR to ensure V1+V2 integration works correctly.

