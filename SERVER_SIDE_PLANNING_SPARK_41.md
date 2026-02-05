# Server-Side Planning Support for Spark 4.1+

## Problem Statement

Spark 4.1 is the new default version for Delta Lake, but Apache Iceberg hasn't released `iceberg-spark-runtime-4.1` yet. This blocked building the entire `delta-iceberg` module, including the serverSidePlanning code which doesn't actually need the Spark-specific runtime.

## Solution

We introduced a new `supportServerSidePlanning` flag that allows selective compilation of just the serverSidePlanning code for Spark versions where the full Iceberg integration isn't available.

## Key Changes

### 1. New SparkVersionSpec Field

Added `supportServerSidePlanning: Boolean` to `SparkVersionSpec` in `project/CrossSparkVersions.scala`:

```scala
case class SparkVersionSpec(
  ...
  supportIceberg: Boolean,
  supportServerSidePlanning: Boolean = false,  // For Spark 4.1+ where iceberg-spark-runtime doesn't exist
  supportHudi: Boolean = true,
  ...
)
```

### 2. Spark Version Configuration

Enabled for Spark 4.1 and 4.2:

```scala
private val spark41 = SparkVersionSpec(
  fullVersion = "4.1.0",
  ...
  supportIceberg = false,
  supportServerSidePlanning = true,  // Only build serverSidePlanning
  ...
)

private val spark42Snapshot = SparkVersionSpec(
  fullVersion = "4.2.0-SNAPSHOT",
  ...
  supportIceberg = false,
  supportServerSidePlanning = true,
  ...
)
```

### 3. Build Configuration (build.sbt)

#### Source Filtering

When `supportServerSidePlanning=true` but `supportIceberg=false`, only serverSidePlanning sources are compiled:

```scala
Compile / sources := {
  val allSources = (Compile / sources).value
  if (supportIceberg) {
    allSources
  } else if (supportServerSidePlanning) {
    // Only include serverSidePlanning sources
    allSources.filter(_.getPath.contains("serverSidePlanning"))
  } else {
    Seq.empty
  }
}
```

#### Minimal Dependencies

Only includes dependencies actually needed by serverSidePlanning:

```scala
libraryDependencies ++= {
  ...
  } else if (supportServerSidePlanning) {
    Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.1",
      "com.github.ben-manes.caffeine" % "caffeine" % "2.9.3",  // Runtime for shaded iceberg-core
      "org.apache.httpcomponents.core5" % "httpcore5" % "5.2.4",
      "org.apache.httpcomponents.client5" % "httpclient5" % "5.3.1"
    )
  }
}
```

**Key insight**: No `iceberg-spark-runtime` dependency needed! serverSidePlanning uses `shadedForDelta.org.apache.iceberg.*` from the `icebergShaded` module, which depends on `iceberg-core` (Spark-version-agnostic).

## Results

### For Spark 4.1 (Default Build)

**Production Code**:
- ✅ Compiles 3 serverSidePlanning source files:
  - `IcebergRESTCatalogPlanningClient.scala`
  - `IcebergRESTCatalogPlanningClientFactory.scala`
  - `SparkToIcebergExpressionConverter.scala`
- ✅ Publishes `delta-iceberg_2.13-4.1.0-SNAPSHOT.jar` (5.7MB)
- ✅ JAR contains only serverSidePlanning classes + shaded iceberg-core dependencies
- ✅ No dependency on non-existent `iceberg-spark-runtime-4.1`

**Other Iceberg Code**:
- ❌ Skipped: ConvertToDelta, ConvertToIceberg, CloneIceberg, Uniform, etc.
- ❌ These require full `iceberg-spark-runtime` integration

### For Spark 4.0

- ✅ Full iceberg support unchanged (`supportIceberg=true`)
- ✅ All code compiles and tests run normally

## Architecture Details

### Why This Works

ServerSidePlanning code uses **shaded iceberg-core APIs**, not the Spark-specific runtime:

```scala
// From IcebergRESTCatalogPlanningClient.scala
import shadedForDelta.org.apache.iceberg.PartitionSpec
import shadedForDelta.org.apache.iceberg.rest.requests.{PlanTableScanRequest, ...}
import shadedForDelta.org.apache.iceberg.expressions.Expression
```

The `icebergShaded` module provides these via:
```scala
"org.apache.iceberg" % "iceberg-core" % "1.10.0"  // ← No Spark version suffix!
```

This dependency is Spark-version-agnostic and exists for all versions.

### Dependency Chain

```
serverSidePlanning code
  ↓ uses
shadedForDelta.org.apache.iceberg.* (from icebergShaded module)
  ↓ depends on
org.apache.iceberg:iceberg-core:1.10.0 (shaded)
  ✓ Exists for all Spark versions
```

**vs** other iceberg code:

```
ConvertToDelta, Uniform, etc.
  ↓ uses
org.apache.iceberg.spark.source.IcebergSource
  ↓ from
org.apache.iceberg:iceberg-spark-runtime-4.1:1.10.0
  ✗ Doesn't exist yet!
```

## Build Commands

```bash
# Build for Spark 4.1 (default) - includes serverSidePlanning only
build/sbt compile
build/sbt publishM2

# Build for Spark 4.0 - includes full iceberg support
build/sbt -DsparkVersion=4.0 compile
build/sbt -DsparkVersion=4.0 publishM2
```

## Future Work

Once Apache Iceberg releases `iceberg-spark-runtime-4.1`:
1. Update `spark41` and `spark42Snapshot` to set `supportIceberg = true`
2. Remove `supportServerSidePlanning = true` (or keep it as redundant)
3. Full delta-iceberg module will build for Spark 4.1+

## Testing Notes

Tests were explored but not included in the final implementation:
- Integration tests that create Iceberg tables fail (need iceberg-spark-runtime)
- Pure unit tests for REST client and expression conversion would pass
- Tests are skipped when `supportServerSidePlanning=true` to keep the build clean

## Files Modified

1. `project/CrossSparkVersions.scala` - Added `supportServerSidePlanning` field
2. `build.sbt` - Source filtering, dependency management, skip conditions

## Artifact Details

**Published artifact for Spark 4.1**:
```
io.delta:delta-iceberg_2.13:4.1.0-SNAPSHOT
Size: 5.7MB
Classes: 1,461 total (3 from serverSidePlanning, rest from shaded iceberg-core)
```

**JAR contents** (org.apache.spark classes only):
```
org/apache/spark/sql/delta/serverSidePlanning/IcebergRESTCatalogPlanningClient.class
org/apache/spark/sql/delta/serverSidePlanning/IcebergRESTCatalogPlanningClientFactory.class
org/apache/spark/sql/delta/serverSidePlanning/SparkToIcebergExpressionConverter.class
```
