# Delta Spark Combined Module

This module contains the final, published `delta-spark` JAR that combines both:
- **V1 (DSv1)**: The traditional Delta Lake connector with full `DeltaLog` support
- **V2 (DSv2)**: The new Kernel-backed connector for improved performance

## Architecture

The combined module provides unified entry points:
- `DeltaCatalog`: Extends `AbstractDeltaCatalog` from the `spark` module
- `DeltaSparkSessionExtension`: Extends `AbstractDeltaSparkSessionExtension` from the `spark` module

## Module Structure

```
spark-combined/          (This module - final published artifact)
  ├── src/main/java/
  │   └── org.apache.spark.sql.delta.catalog.DeltaCatalog.java
  └── src/main/scala/
      └── io.delta.sql.DeltaSparkSessionExtension.scala

spark/                   (sparkV1 - V1 implementation)
  ├── Core Delta Lake classes with DeltaLog
  └── AbstractDeltaCatalog, AbstractDeltaSparkSessionExtension

kernel-spark/            (sparkV2 - V2 implementation)
  └── Kernel-backed DSv2 connector
```

## How It Works

1. **sparkV1** (`spark/`): Contains production code for the V1 connector including DeltaLog
2. **sparkV1Filtered** (`spark-v1-shaded/`): Filtered version of V1 excluding DeltaLog, Snapshot, OptimisticTransaction, and actions.scala
3. **sparkV2** (`kernel-spark/`): Kernel-backed V2 connector that depends on sparkV1Filtered
4. **spark** (this module): Final JAR that merges V1 + V2 + storage classes

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
4. Exports as JAR to avoid classpath conflicts

## Testing

Tests are located in `spark/src/test/` and run against the combined JAR to ensure V1+V2 integration works correctly.

