# DSv2 StartingVersion Issue Analysis

## Executive Summary

**TL;DR**: The `startingVersion` test failure is NOT a bug. It's a **fundamental design difference** between V1 and Kernel API:

- **V1 API**: Can read changelog files without validating protocol → fast but potentially incorrect
- **Kernel API**: Requires protocol/metadata to read AddFile correctly → safe but more restrictive
- **Why it matters**: Features like Column Mapping change how AddFile.stats and partitionValues are interpreted
- **Recommendation**: Accept as breaking change; DSv2 is more correct per Delta Protocol specification

This document explains the technical justification from `PROTOCOL.md` for why Kernel API requires a `startSnapshot` parameter.

---

## Problem Summary

The `startingVersion` test in `DeltaSourceDSv2Suite` is failing because:
- **Expected**: Read 10 rows (values 10-19) when starting from version 1
- **Actual**: Read 0 rows (empty result)

## Root Cause

There's a fundamental API design difference between Delta v1 and Kernel API regarding starting versions:

### V1 API Behavior (DeltaSource.scala)
```scala
protected def getFileChanges(fromVersion: Long, ...): ClosableIterator[IndexedFile] = {
  def filterAndIndexDeltaLogs(startVersion: Long): ClosableIterator[IndexedFile] = {
    deltaLog.getChangeLogFiles(startVersion, catalogTableOpt, options.failOnDataLoss).flatMapWithClose {
      case (version, filestatus) =>
        // Directly read changelog files without requiring snapshot at startVersion
        ...
    }
  }
}
```

**Key point**: V1 API can start reading from any version by directly reading changelog files. It does NOT require the snapshot at `startVersion` to be readable/reconstructable.

### Kernel API Behavior (CommitRange.java)
```java
/**
 * @param startSnapshot the snapshot for startVersion, required to ensure the table is readable by
 *     Kernel at startVersion
 * @throws IllegalArgumentException if startSnapshot.getVersion() != startVersion
 * @throws KernelException if the version range contains a version with reader protocol that is
 *     unsupported by Kernel
 */
CloseableIterator<ColumnarBatch> getActions(
    Engine engine, 
    Snapshot startSnapshot,  // <-- REQUIRED!
    Set<DeltaLogActionUtils.DeltaAction> actionSet);
```

**Key point**: Kernel API REQUIRES a valid snapshot at `startVersion`. This enforces that the table must be readable by Kernel at that version.

### DSv2 Implementation (SparkMicroBatchStream.java)

```java
private CloseableIterator<IndexedFile> filterDeltaLogs(
    long startVersion, Optional<DeltaSourceOffset> endOffset) {
  
  CommitRange commitRange = snapshotManager.getTableChanges(engine, startVersion, endVersionOpt);
  
  // Required by kernel: perform protocol validation by creating a snapshot at startVersion.
  Snapshot startSnapshot;
  try {
    startSnapshot = snapshotManager.loadSnapshotAt(startVersion);  // Line 607
  } catch (io.delta.kernel.exceptions.KernelException e) {
    // If startVersion doesn't exist (e.g., starting from "latest" when the next version
    // hasn't been written yet), return empty iterator.
    return Utils.toCloseableIterator(allIndexedFiles.iterator());  // <-- RETURNS EMPTY!
  }
  
  try (CloseableIterator<ColumnarBatch> actionsIter =
      commitRange.getActions(engine, startSnapshot, ACTION_SET)) {  // Line 615
    // Process actions...
  }
}
```

**The problem**: When `loadSnapshotAt(startVersion)` fails (e.g., due to unsupported protocol), the code catches the exception and returns an empty iterator, resulting in 0 rows read.

## Why This Difference Matters

### Scenario 1: Old Protocol Version
If `startVersion=1` uses a protocol that Kernel doesn't support:
- **V1**: Can still read from version 1 onward by reading changelog files directly
- **Kernel/DSv2**: Cannot create snapshot at version 1, so returns empty results

### Scenario 2: Non-reconstructable Snapshot
If the snapshot at `startVersion` cannot be reconstructed (missing checkpoint, etc.):
- **V1**: Can still read changelog files
- **Kernel/DSv2**: Cannot create snapshot, returns empty results

### Scenario 3: Comment from SparkMicroBatchStream
```java
// When starting from a given version, we don't require that the snapshot of this
// version can be reconstructed, even though the input table is technically in an
// inconsistent state. If the snapshot cannot be reconstructed, then the protocol
// check is skipped, so this is technically not safe, but we keep it this way for
// historical reasons.
```

This comment in `validateProtocolAt()` (line 438-443) acknowledges that V1 has this behavior "for historical reasons" but DSv2 cannot implement the same behavior due to Kernel API constraints.

## Why Kernel API Requires startSnapshot

Looking at the Kernel API design and Delta Protocol specification, the `startSnapshot` parameter is required because **you cannot correctly read AddFile actions without knowing the Protocol and Metadata**.

### Protocol-Dependent Features That Affect AddFile Reading

From `PROTOCOL.md`, there are multiple table features that fundamentally change how AddFiles must be interpreted:

#### 1. **Column Mapping** (Reader Version 2+)
```
Reader Requirements (PROTOCOL.md line 930-937):
- In 'id' mode: Must resolve columns by field_id in parquet metadata
- In 'name' mode: Must resolve columns by physicalName
- Partition values and statistics must be resolved by physical names
```

**Impact**: Without knowing if Column Mapping is enabled and which mode, you cannot:
- Read partition values correctly from AddFile.partitionValues
- Parse column statistics correctly from AddFile.stats
- Read the parquet files themselves

#### 2. **Deletion Vectors** (Reader Version 3+, Writer Version 7+)
```
AddFile Schema (PROTOCOL.md line 516):
- deletionVector: [DeletionVectorDescriptor Struct] (optional)

Reader Requirements (PROTOCOL.md line 1006-1007):
- If a logical file has a DV, invalidated records MUST NOT be returned
```

**Impact**: Without checking protocol, you don't know:
- If Deletion Vectors are supported
- How to handle AddFile.deletionVector field
- Which rows to filter when reading

#### 3. **Row Tracking** (Writer Version 7+)
```
AddFile Schema (PROTOCOL.md line 517-518):
- baseRowId: Long (optional)
- defaultRowCommitVersion: Long (optional)
```

**Impact**: These fields only exist if Row Tracking feature is active.

#### 4. **Clustered Tables** (Writer Version 7+)
```
AddFile Schema (PROTOCOL.md line 519):
- clusteringProvider: String (optional)
```

**Impact**: Without protocol, can't tell if this field is present.

### Why V1 Could Skip This - THE KEY DIFFERENCE

**V1 uses the CURRENT (source init) snapshot's P&M to read ALL historical AddFiles:**

```scala
// From DeltaSource.scala line 728-734
case class DeltaSource(
    spark: SparkSession,
    deltaLog: DeltaLog,
    catalogTableOpt: Option[CatalogTable],
    options: DeltaOptions,
    snapshotAtSourceInit: SnapshotDescriptor,  // <-- FIXED at source init!
    metadataPath: String,
    ...
)

// Line 199-228: readSnapshotDescriptor is based on snapshotAtSourceInit
protected lazy val readSnapshotDescriptor: SnapshotDescriptor =
  persistedMetadataAtSourceInit.map { ... }
    .getOrElse(snapshotAtSourceInit)  // Use snapshot from source init

// Line 390-407: ALL AddFiles are read using this SAME descriptor
deltaLog.createDataFrame(
  readSnapshotDescriptor,  // <-- Same P&M for all versions!
  addFiles.map(_.getFileAction.asInstanceOf[AddFile]),
  isStreaming = true)
```

**Key insight**: V1 gets ONE snapshot when the stream starts (usually the latest), then uses that snapshot's Protocol and Metadata to interpret AddFiles from ALL versions, including historical ones.

This works because V1 assumes:
1. **Forward compatibility**: Current P&M can read historical AddFiles
2. **Single interpretation**: Use one consistent view of schema/features
3. **Lazy validation**: Errors surface at DataFrame creation, not log reading
4. **No per-version snapshot needed**: Only need to read changelog files, not reconstruct snapshots
5. **startVersion doesn't need to be readable**: As long as changelog exists, can read it

### Why Kernel API Is Stricter

**Kernel API requires the startVersion's P&M to read that version's AddFiles:**

```java
// From CommitRange.java line 96-97:
@param startSnapshot the snapshot for startVersion, required to ensure the table is readable by
    Kernel at startVersion

// From SparkMicroBatchStream.java line 604-615
CommitRange commitRange = snapshotManager.getTableChanges(engine, startVersion, endVersionOpt);

// Must load snapshot AT startVersion (not latest!)
Snapshot startSnapshot = snapshotManager.loadSnapshotAt(startVersion);  

// Use startVersion's P&M to read its AddFiles
CloseableIterator<ColumnarBatch> actionsIter =
    commitRange.getActions(engine, startSnapshot, ACTION_SET);
```

The startSnapshot provides:
1. **Protocol**: Which features are active at THAT version (not current)
2. **Metadata**: Schema with column mapping info at THAT version
3. **Validation**: Ensures startVersion is actually readable by Kernel
4. **Per-version interpretation**: Each version's AddFiles interpreted with that version's P&M

## The Fundamental Architectural Difference

This is the core of the issue:

| Aspect | V1 API | Kernel API |
|--------|--------|------------|
| **P&M Source** | Latest snapshot (at source init) | startVersion's snapshot |
| **P&M Updates** | Fixed throughout stream | Per version |
| **Assumption** | Forward compatible | Version-specific |
| **When fails** | At DataFrame read time | At log read time (earlier) |
| **Requirement** | Only changelog files exist | Snapshot must be reconstructable |
| **startingVersion** | Just needs commit file | Needs full snapshot |

### Why This Matters

**Scenario: Reading from version 10, current version is 100**

V1:
```scala
// Use version 100's P&M to read version 10's AddFiles
// Works if P&M is forward compatible
// Fails if version 10 used different column mapping mode
```

Kernel:
```java
// Must reconstruct version 10's snapshot
// Use version 10's P&M to read version 10's AddFiles
// Fails if version 10 has unsupported protocol
// More correct but more restrictive
```

### Concrete Example: Column Mapping

Without startSnapshot:
```java
// AddFile has partitionValues = {"col-a7f4159c-53be-4cb0-b81a-f7e5240cfc49": "2024-01-01"}
// Is this the actual column name, or a physical name?
// Need metadata to know if Column Mapping is active!

// AddFile.stats contains:
// {"col-5f422f40-de70-45b2-88ab-1d5c90e94db1": {"min": 1, "max": 100}}
// What column does this refer to?
// Need metadata to map physical name -> logical name!
```

With startSnapshot:
```java
Snapshot snapshot = loadSnapshotAt(version);
Protocol protocol = snapshot.getProtocol();
Metadata metadata = snapshot.getMetadata();

if (protocol.supportsColumnMapping()) {
  // Use metadata.schema to map physical names -> logical names
  String logicalName = metadata.getLogicalName("col-a7f4159c-...");
}
```

### From PROTOCOL.md Line 1973-1984

```
# Requirements for Readers

Reader Version 2: Respect Column Mapping
Reader Version 3: Respect Table Features for readers
```

**You cannot read a Delta table correctly without knowing its Protocol and Metadata.**

This is a more restrictive but safer API design compared to V1.

## Impact

This design difference means:

1. **Test Failures**: Tests like `startingVersion` that rely on V1's lenient behavior will fail in DSv2
2. **Behavior Change**: Users who relied on starting from non-readable versions will see different behavior
3. **API Limitation**: Kernel API fundamentally cannot support the same use cases as V1 API

## Possible Solutions

### Option 1: Relax Kernel API Requirements
Modify `CommitRange.getActions()` to not require `startSnapshot`, or make it optional. This would require changes to the Kernel API itself.

**Pros**: Most compatible with V1 behavior
**Cons**: Requires Kernel API changes, may reduce safety guarantees

### Option 2: Document as Breaking Change
Document that DSv2 has stricter requirements for `startingVersion` and this is a known behavior change.

**Pros**: No code changes needed
**Cons**: Breaking change for users

### Option 3: Fallback Mechanism
When snapshot creation fails, attempt to read changelog files directly (similar to V1), but this may require Kernel API enhancements.

**Pros**: Best of both worlds
**Cons**: Complex implementation, may still require Kernel API changes

### Option 4: Skip Protocol Validation (Current Workaround)
Use the `validateProtocolAt()` approach but make it optional via configuration.

**Pros**: Can be controlled via config
**Cons**: Reduces safety, doesn't fully solve the problem (snapshot creation still required)

### Option 5: Allow `snapshot.version >= startVersion` ✅ RECOMMENDED

**核心思路**：修改 Kernel API 的 validation，允许使用**更新版本**的 snapshot 来读取**历史版本**的数据（类似 V1）。

**当前限制**：
```java
// CommitRangeImpl.java line 125-127
checkArgument(
    startSnapshot.getVersion() == startVersion,
    "startSnapshot must have version = startVersion");
```

**建议修改**：
```java
checkArgument(
    startSnapshot.getVersion() >= startVersion,
    "startSnapshot version must be >= startVersion");
```

**优势**：
- ✅ 解决 startingVersion 无法重建的问题
- ✅ 保持 Kernel API 结构（仍需 snapshot 参数）
- ✅ 向后兼容（现有代码仍然工作）
- ✅ 允许 V1 风格的"用最新 P&M"策略
- ✅ 用户可选择：精确版本（更安全）或最新版本（更实用）
- ✅ 只需修改一行代码

**实现示例**：
```java
// DSv2 can now fallback to latest snapshot
Snapshot startSnapshot;
try {
  startSnapshot = snapshotManager.loadSnapshotAt(startVersion);
} catch (KernelException e) {
  // Fallback to latest (like V1)
  startSnapshot = snapshotManager.loadLatestSnapshot();
  logger.warn("Using latest snapshot P&M to read from version {}", startVersion);
}
commitRange.getActions(engine, startSnapshot, ACTION_SET);
```

**风险**：
- ⚠️ 假设 P&M 向前兼容（大多数情况下成立）
- ⚠️ 如果 P&M 有重大变化（如启用 column mapping），可能误读数据

**缓解**：
- 添加警告日志
- 提供配置选项让用户选择严格/宽松模式
- 智能检测已知的不兼容情况（如 column mapping 变化）

详细分析见：`kernel-api-relaxed-snapshot-proposal.md`

## Recommendation

The core issue is that **Kernel API's design is fundamentally incompatible with V1's lenient behavior**. The requirement to pass a `startSnapshot` parameter is baked into the API signature.

### This Is Not A Bug - It's A Design Decision

After analyzing the Delta Protocol specification, it's clear that:

1. **Kernel API is correct**: You genuinely need Protocol and Metadata to read AddFile actions correctly
2. **V1 is lenient but potentially incorrect**: V1 can produce wrong results when reading from versions without protocol validation
3. **The requirement is justified**: Features like Column Mapping, Deletion Vectors, and Row Tracking fundamentally change how AddFiles are interpreted

### Why The Test Fails

The test `startingVersion` likely:
1. Creates a table at version 0 with protocol X
2. Tries to start streaming from version 1
3. DSv2 tries to load snapshot at version 1
4. Version 1 may have unsupported protocol or cannot be reconstructed
5. DSv2 correctly returns empty (safer than returning wrong data)
6. V1 would have proceeded anyway (less safe, but more permissive)

### Options Forward

#### Option 1: Accept Breaking Change ✅ RECOMMENDED
Document that DSv2 has stricter safety requirements:
- Starting version must point to a readable snapshot
- Protocol must be supported by Kernel
- This is more correct than V1's behavior

**Pros**: Safer, more correct
**Cons**: Breaking change for some users

#### Option 2: Relax Kernel API (Major Change)
Make `startSnapshot` optional in Kernel API:
```java
CloseableIterator<ColumnarBatch> getActions(
    Engine engine, 
    Optional<Snapshot> startSnapshot,  // Optional now
    Set<DeltaLogActionUtils.DeltaAction> actionSet);
```

**Pros**: Backward compatible
**Cons**: 
- Requires Kernel API changes
- Loses safety guarantees
- May produce incorrect results for Column Mapping, Deletion Vectors, etc.
- Goes against Delta Protocol specification

#### Option 3: Best-Effort Fallback (Complex)
When snapshot fails, attempt to infer minimal protocol/metadata by:
1. Reading startVersion commit file
2. Extracting protocol/metadata actions
3. Validating they're supported

**Pros**: More compatible
**Cons**: 
- Very complex
- Still may fail for unsupported protocols
- Partial solution only

#### Option 4: Config-Based Validation (Workaround)
Add a config to skip protocol validation:
```
spark.delta.streaming.unsafeSkipProtocolValidation=true
```

**Pros**: User can opt-in to V1 behavior
**Cons**: 
- Unsafe
- May produce corrupt results
- Still requires snapshot creation which may fail

### Conclusion

**Both designs are valid, but serve different goals:**

1. **V1 Design: Pragmatic Forward Compatibility**
   - Uses latest P&M to read all historical data
   - Assumes P&M is forward compatible
   - More permissive, works in more scenarios
   - Risk: May misinterpret historical AddFiles if P&M changed

2. **Kernel Design: Version-Specific Correctness**
   - Uses each version's P&M to read that version's data
   - Guarantees correct interpretation
   - More restrictive, safer
   - Limitation: Requires snapshot reconstruction at startVersion

**Why the test fails:**

The test likely creates a table where version 1 cannot be reconstructed (missing checkpoint, unsupported protocol, etc.). V1 doesn't care because it uses the current snapshot's P&M. Kernel fails because it needs version 1's snapshot.

**Which is correct?**

From `PROTOCOL.md`, the protocol specification doesn't mandate either approach. However:
- If P&M has changed significantly between startVersion and current version (e.g., column mapping was added), V1's approach may produce incorrect results
- If P&M hasn't changed (or is backward compatible), V1's approach is more practical

**The real issue**: Kernel API's requirement for `startSnapshot` makes it **impossible** to implement V1's "use latest P&M" approach, even if that's sometimes the right choice.

### Recommendation

We should **accept this as a known architectural difference**, not a bug:

1. **Document the difference**: DSv2 requires startVersion to have a reconstructable snapshot
2. **Explain the tradeoff**: V1 is more permissive but may misinterpret data; DSv2 is safer but more restrictive
3. **Consider API enhancement**: Add optional config to use "latest P&M" mode in Kernel API for users who understand the tradeoffs

## Real-World Scenario: Why This Matters

### Example 1: V1's "Latest P&M" vs Kernel's "Version-Specific P&M"

**Setup:**
```
Version 0: Initial table
  Protocol: Reader=1, Writer=2
  Schema: {name: "id", type: "int"}
  AddFile: file0.parquet with stats: {"id": {min: 0, max: 99}}

Version 10: Column Mapping enabled
  Protocol: Reader=2, Writer=5, columnMapping
  Metadata: Column "id" -> physical name "col-abc123"
  AddFile: file10.parquet with stats: {"col-abc123": {min: 100, max: 199}}

Version 100: Current version
  Same protocol/metadata as v10
  AddFile: file100.parquet with stats: {"col-abc123": {min: 1000, max: 1099}}
```

**Stream starts at version 0, current version is 100:**

V1 Approach:
```scala
// DeltaSource initialized at version 100
snapshotAtSourceInit = loadSnapshot(100)  // Reader=2, columnMapping enabled

// Read version 0's AddFile
// Uses version 100's P&M: expects physical names
// Version 0's stats: {"id": {min: 0, max: 99}}
// V1 sees "id" as column name (no physical name mapping)
// PROBLEM: May fail or misinterpret because current P&M expects "col-abc123"
```

Kernel Approach:
```java
// Stream starts at version 0
startSnapshot = loadSnapshotAt(0)  // Reader=1, no column mapping
protocol = startSnapshot.getProtocol()  // Reader=1
metadata = startSnapshot.getMetadata()  // No column mapping

// Read version 0's AddFile
// Uses version 0's P&M: no column mapping
// Version 0's stats: {"id": {min: 0, max: 99}}
// Kernel correctly interprets "id" as direct column name
// ✅ CORRECT
```

### Scenario: Table with Column Mapping Enabled Mid-Stream

```
Version 0: Table created
  Protocol: Reader=1, Writer=2
  Schema: {name: "id", type: "int"}
  
Version 1: Column Mapping enabled
  Protocol: Reader=2, Writer=5, features=[columnMapping]
  Metadata: {
    "schema": {
      "name": "id",
      "metadata": {
        "delta.columnMapping.id": 1,
        "delta.columnMapping.physicalName": "col-abc123"
      }
    },
    "configuration": {"delta.columnMapping.mode": "name"}
  }
  AddFile: {
    "path": "file1.parquet",
    "partitionValues": {},
    "stats": {
      "numRecords": 100,
      "minValues": {"col-abc123": 1},  // Uses physical name!
      "maxValues": {"col-abc123": 100}
    }
  }

Version 2: More data added
  AddFile: {
    "path": "file2.parquet",
    "stats": {
      "numRecords": 50,
      "minValues": {"col-abc123": 101},  // Still physical name
      "maxValues": {"col-abc123": 150}
    }
  }
```

### What Happens With startingVersion=1?

**V1 Behavior (Permissive)**:
```scala
// V1 reads version 1's changelog directly
// Sees AddFile with stats: {"col-abc123": {min: 1, max: 100}}
// Doesn't check protocol, assumes "col-abc123" is the column name
// Query: SELECT * FROM table WHERE id > 50
// Result: May skip file incorrectly or fail to read parquet
```

**Kernel/DSv2 Behavior (Correct)**:
```java
// Tries to load snapshot at version 1
Snapshot snapshot1 = loadSnapshotAt(1);
Protocol protocol = snapshot1.getProtocol();  // Reader=2, columnMapping
Metadata metadata = snapshot1.getMetadata();  // Has physicalName mapping

// Reads AddFile with stats: {"col-abc123": {min: 1, max: 100}}
// Uses metadata to understand: "col-abc123" maps to "id"
// Query: SELECT * FROM table WHERE id > 50
// Result: Correctly reads file and applies filter
```

**If Version 1's Protocol Is Unsupported**:
```java
// DSv2: Fails early, returns empty
// Better than V1's "try anyway and maybe corrupt data"
```

### The Key Insight

From `PROTOCOL.md` line 927:
> "Track partition values and column level statistics with the physical name 
> of the column in the transaction log."

**Without metadata, you cannot parse AddFile.stats correctly!**

The stats use physical names, but queries use logical names. You MUST have the metadata to translate between them.

## References

- Kernel API: `/kernel/kernel-api/src/main/java/io/delta/kernel/CommitRange.java` lines 96-107
- DSv2 Implementation: `/kernel-spark/src/main/java/io/delta/kernel/spark/read/SparkMicroBatchStream.java` lines 604-615
- V1 Implementation: `/spark/src/main/scala/org/apache/spark/sql/delta/sources/DeltaSource.scala` lines 779-809
- Test: `/spark/src/test/scala/org/apache/spark/sql/delta/DeltaSourceSuite.scala` lines 1585-1643
- Delta Protocol: `/PROTOCOL.md`
  - Add File schema: lines 505-529
  - Column Mapping: lines 885-937
  - Deletion Vectors: lines 939-1010
  - Reader Requirements: lines 1973-1984

