# POC: DistributedScanBuilder Architecture

## Overview

This POC demonstrates a cleaner architecture where distributed log replay is encapsulated in Kernel-compatible ScanBuilder/Scan classes.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│ SparkScan (No Changes Needed!)                          │
│   - Uses standard Kernel Scan API                       │
│   - Doesn't know about serial vs distributed            │
└────────────────┬────────────────────────────────────────┘
                 │
                 ├─→ KernelScanBuilder (serial - original)
                 │   └─→ KernelScan
                 │       └─→ getScanFiles() 
                 │           → Iterator<FilteredColumnarBatch>
                 │
                 └─→ DistributedScanBuilder (new POC) ✨
                     ├─ DataFrame member (distributed processing)
                     ├─ withFilter() → df.filter()
                     ├─ withReadSchema() → df.select()
                     └─→ build() → DistributedScan
                         └─→ getScanFiles()
                             └─→ DataFrameColumnarBatch
                                 └─→ SparkRowWrapper
```

## Implementation

### 1. DistributedScanBuilder
**File**: `DistributedScanBuilder.java`

```java
// Maintains DataFrame internally
private Dataset<Row> dataFrame;

// Initialize with distributed log replay
this.dataFrame = DistributedLogReplayHelper.stateReconstructionV2(
    spark, snapshot, numPartitions);

// Translate Kernel operations to DataFrame operations
@Override
public ScanBuilder withFilter(Predicate predicate) {
    dataFrame = dataFrame.filter(convertToSparkColumn(predicate));
    return this;
}
```

### 2. DistributedScan
**File**: `DistributedScan.java`

```java
// Implements Kernel Scan interface
@Override
public CloseableIterator<FilteredColumnarBatch> getScanFiles(Engine engine) {
    List<Row> sparkRows = dataFrame.collectAsList();
    return new DataFrameColumnarBatchIterator(sparkRows, snapshot, readSchema);
}
```

### 3. DataFrameColumnarBatch
**File**: `DataFrameColumnarBatch.java`

```java
// Wrapper: Implements FilteredColumnarBatch
class DataFrameColumnarBatch implements FilteredColumnarBatch {
    private final ColumnarBatch columnarBatch;
    
    // Wraps Spark Rows as Kernel ColumnarBatch
    public DataFrameColumnarBatch(List<Row> sparkRows, StructType schema) {
        this.columnarBatch = new SparkRowsColumnarBatch(sparkRows, schema);
    }
}
```

### 4. SparkRowWrapper
**File**: `SparkRowWrapper.java`

```java
// Critical adapter: Spark Row → Kernel Row
class SparkRowWrapper implements Row {
    private final org.apache.spark.sql.Row sparkRow;
    
    @Override
    public String getString(int ordinal) {
        return sparkRow.getString(ordinal);
    }
    // ... other type accessors
}
```

## Usage Example

### Before (Current Implementation)
```java
// SparkScan needs to know about distributed implementation
private void planScanFiles() {
    Dataset<Row> df = DistributedLogReplayHelper.stateReconstructionV2(...);
    List<Row> rows = df.collectAsList();
    for (Row row : rows) {
        // Manual conversion
    }
}
```

### After (POC Architecture)
```java
// SparkScan uses standard Kernel API - clean!
ScanBuilder scanBuilder = new DistributedScanBuilder(spark, snapshot, 50)
    .withFilter(predicate)           // ← Translated to df.filter()
    .withReadSchema(projectedSchema); // ← Translated to df.select()

Scan scan = scanBuilder.build();

// Standard Kernel API - no knowledge of DataFrame
Iterator<FilteredColumnarBatch> files = scan.getScanFiles(engine);
while (files.hasNext()) {
    FilteredColumnarBatch batch = files.next();
    // Process as usual
}
```

## Key Benefits

### 1. Clean Abstraction
- ✅ SparkScan doesn't need to change
- ✅ Uses standard Kernel API (ScanBuilder pattern)
- ✅ No knowledge of distributed vs serial implementation

### 2. Easy Extension
```java
// Add new operations easily
@Override
public ScanBuilder withPartitionFilter(Expression expr) {
    dataFrame = dataFrame.where(convertPartitionFilter(expr));
    return this;
}

@Override
public ScanBuilder withLimit(long limit) {
    dataFrame = dataFrame.limit((int) limit);
    return this;
}
```

### 3. Testability
```java
// Easy to unit test
@Test
public void testDistributedScanBuilderWithFilter() {
    DistributedScanBuilder builder = new DistributedScanBuilder(spark, snapshot, 50);
    builder.withFilter(new EqualTo("age", Literal.of(18)));
    
    Scan scan = builder.build();
    // Verify filtering worked correctly
}
```

### 4. Flexibility
```java
// Easy to switch between serial and distributed
ScanBuilder builder;
if (isLargeTable()) {
    builder = new DistributedScanBuilder(spark, snapshot, 50);
} else {
    builder = snapshot.getScanBuilder(); // Kernel's serial
}

// Rest of code is identical
Scan scan = builder.withFilter(pred).build();
```

## Implementation Status

### ✅ Completed in POC
1. **DistributedScanBuilder** - ScanBuilder implementation
2. **DistributedScan** - Scan implementation
3. **DataFrameColumnarBatch** - FilteredColumnarBatch wrapper
4. **SparkRowWrapper** - Row adapter (Spark → Kernel)
5. **DataFrameColumnarBatchIterator** - Iterator implementation

### ⚠️ TODO for Production
1. **Predicate Conversion** - Full Kernel Predicate → Spark Column
2. **Array/Map Support** - Complete SparkRowWrapper for complex types
3. **Error Handling** - Robust error handling and logging
4. **Performance Tuning** - Optimize batch size, conversion overhead
5. **Testing** - Comprehensive unit and integration tests

## Performance Considerations

### Zero-Copy Design
```java
// SparkRowWrapper doesn't copy data
class SparkRowWrapper implements Row {
    private final org.apache.spark.sql.Row sparkRow; // Just wrap, don't copy
    
    @Override
    public String getString(int ordinal) {
        return sparkRow.getString(ordinal); // Direct access
    }
}
```

### Batching Strategy
```java
// Configurable batch size (default 1000 rows)
private static final int DEFAULT_BATCH_SIZE = 1000;

// Trade-off:
// - Smaller batches: less memory, more overhead
// - Larger batches: more memory, less overhead
```

### Memory Efficiency
```java
// Collect is done AFTER filtering/projection on executors
Dataset<Row> df = DistributedLogReplayHelper.stateReconstructionV2(...)
    .filter(sparkFilter)    // ← On executors
    .select(projectedCols);  // ← On executors

List<Row> rows = df.collectAsList(); // ← Only filtered data to driver
```

## Comparison with Current Implementation

| Aspect | Current | POC Architecture |
|--------|---------|------------------|
| **Abstraction** | SparkScan knows details | Clean Kernel API |
| **Extensibility** | Hard to add features | Easy (ScanBuilder) |
| **Testability** | Coupled to SparkScan | Isolated, easy to test |
| **Code Location** | Spread across files | Well encapsulated |
| **API Compatibility** | Custom | Standard Kernel API |
| **Performance** | Good | Similar (one extra layer) |

## Migration Path

### Phase 1: Validate POC (Now)
```bash
# Compile POC
cd /Users/xin.huang/oss/delta
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home
build/sbt "project sparkV2" "compile"
```

### Phase 2: Integrate (Optional)
```java
// Modify SparkScan to use DistributedScanBuilder
ScanBuilder scanBuilder;
if (useDistributed) {
    scanBuilder = new DistributedScanBuilder(spark, initialSnapshot, 50);
} else {
    scanBuilder = initialSnapshot.getScanBuilder();
}

Scan scan = scanBuilder.withFilter(predicate).build();
// Rest stays the same
```

### Phase 3: Complete (Future)
- Implement full predicate conversion
- Add comprehensive tests
- Performance benchmarks
- Documentation

## Recommendation

### For Immediate Use
**Keep current implementation** - It's working, tested, and production-ready.

### For Future Enhancement
**Adopt POC architecture** when:
1. Need to add more scan features (limit, projection, etc.)
2. Want cleaner separation of concerns
3. Ready to invest in refactoring

## Files Created

```
spark/v2/src/main/java/io/delta/spark/internal/v2/read/
├── DistributedScanBuilder.java           (141 lines) ✨
├── DistributedScan.java                   (56 lines) ✨
├── DataFrameColumnarBatch.java            (127 lines) ✨
├── SparkRowWrapper.java                   (214 lines) ✨
└── DataFrameColumnarBatchIterator.java     (75 lines) ✨

spark/v2/
└── DISTRIBUTED_SCAN_POC.md               (This file) ✨
```

## Summary

✅ **POC demonstrates cleaner architecture**  
✅ **Follows Kernel design patterns**  
✅ **Easy to extend with new features**  
✅ **Zero-copy, memory efficient**  
⚠️ **Needs predicate conversion for production**

**Next Step**: Compile and validate the POC!
