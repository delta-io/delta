# Netty Dependency Conflict Analysis

## The Problem

**Armeria 1.28.4** (used by Unity Catalog 0.3.1) requires **Netty API from ~4.1.100** range with the new constructor:
```java
DefaultHttpMessage.<init>(HttpVersion, HttpHeadersFactory)
```

But your classpath has **Netty 4.1.118.Final** which has breaking changes incompatible with Armeria 1.28.4.

## Root Cause: Shaded/Bundled Netty

The real problem is **`bundle-2.24.6.jar`** from AWS SDK v2, which contains a **shaded/bundled copy** of Netty inside it.

### The Split-Brain Problem

Stack trace shows TWO different Netty sources:
```
~[bundle-2.24.6.jar:?]                                    ← AWS SDK's bundled Netty
~[netty-transport-classes-kqueue-4.1.111.Final.jar:...]   ← Explicit Netty dependency
```

This creates incompatible class versions at runtime:
- Some Netty classes load from the bundle (incompatible version)
- Other Netty classes load from explicit JARs (4.1.111)
- Result: `NoSuchMethodError` when they interact

## Who's Importing Conflicting Netty Versions?

### 1. **Unity Catalog Server 0.3.1** → Netty 4.1.111.Final ✓
- Declares: `io.netty:netty-all:4.1.111.Final`
- This version SHOULD work with Armeria 1.28.4

### 2. **Apache Spark 4.0.0** → Netty 4.1.118.Final 
The following Spark modules bring in Netty 4.1.118.Final:
- `spark-core_2.13`
- `spark-sql_2.13`
- `spark-catalyst_2.13`
- `spark-connect_2.13`
- `spark-connect-client-jvm_2.13`
- `spark-connect-common_2.13`
- `spark-network-common_2.13`
- `spark-sql-api_2.13`
- `spark-hive_2.13`

### 3. **AWS SDK Bundle** → Shaded/Bundled Netty (THE REAL CULPRIT!)
- `software.amazon.awssdk:bundle:2.24.6` 
- Contains **shaded Netty classes** that cannot be overridden by `dependencyOverrides`
- Comes transitively through `hadoop-aws`

## Why 4.1.118 Breaks Armeria 1.28.4

Netty 4.1.118.Final made API changes that are incompatible with what Armeria 1.28.4 was compiled against.
Specifically, the `DefaultHttpMessage` constructor signature changed or the `HttpHeadersFactory` parameter handling changed.

## Solution: Two-Part Fix

### Part 1: Force Netty 4.1.111.Final Override

Add `dependencyOverrides` to force all explicit Netty artifacts to 4.1.111.Final:

```scala
dependencyOverrides ++= Seq(
  // ... existing Jackson overrides ...
  
  // Force Netty 4.1.111.Final (Unity Catalog's version) to avoid Armeria 1.28.4 incompatibility
  "io.netty" % "netty-all" % "4.1.111.Final",
  "io.netty" % "netty-handler" % "4.1.111.Final",
  "io.netty" % "netty-transport" % "4.1.111.Final",
  "io.netty" % "netty-codec" % "4.1.111.Final",
  "io.netty" % "netty-codec-http" % "4.1.111.Final",
  "io.netty" % "netty-codec-http2" % "4.1.111.Final",
  "io.netty" % "netty-common" % "4.1.111.Final",
  "io.netty" % "netty-buffer" % "4.1.111.Final",
  "io.netty" % "netty-resolver" % "4.1.111.Final",
  "io.netty" % "netty-transport-classes-kqueue" % "4.1.111.Final",
  "io.netty" % "netty-transport-native-kqueue" % "4.1.111.Final",
  "io.netty" % "netty-transport-classes-epoll" % "4.1.111.Final",
  "io.netty" % "netty-transport-native-epoll" % "4.1.111.Final",
  "io.netty" % "netty-transport-native-unix-common" % "4.1.111.Final"
)
```

### Part 2: Exclude AWS SDK Bundle and Add Individual Modules

```scala
// Exclude the AWS SDK bundle which contains shaded Netty
"org.apache.hadoop" % "hadoop-aws" % hadoopVersion % "test" excludeAll(
  ExclusionRule(organization = "software.amazon.awssdk", name = "bundle")
),

// Add individual AWS SDK v2 dependencies to replace the excluded bundle
// These will use our Netty 4.1.111.Final from dependencyOverrides
"software.amazon.awssdk" % "s3" % "2.24.6" % "test",
"software.amazon.awssdk" % "sts" % "2.24.6" % "test",
"software.amazon.awssdk" % "dynamodb" % "2.24.6" % "test",
```

## Why This Works

1. **Excluding the bundle** eliminates the shaded/bundled Netty that was causing the split-brain
2. **Adding individual AWS SDK modules** restores AWS functionality but they use explicit Netty dependencies
3. **dependencyOverrides forces 4.1.111** for all explicit Netty artifacts
4. **Result:** Single, consistent Netty version (4.1.111.Final) throughout the classpath

## Testing

After applying the fix, verify:
1. Unity Catalog server starts without `NoSuchMethodError`
2. No more `bundle-2.24.6.jar` references in error stack traces
3. Spark functionality still works
4. AWS S3 operations work (if tested)
5. Tests pass

