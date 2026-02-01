# POC Integration Example

## Current Status

✅ **All POC files created**:
1. `DistributedScanBuilder.java` - ScanBuilder implementation
2. `DistributedScan.java` - Scan implementation  
3. `DataFrameColumnarBatch.java` - FilteredColumnarBatch wrapper
4. `SparkRowWrapper.java` - Spark Row → Kernel Row adapter
5. `DataFrameColumnarBatchIterator.java` - Iterator implementation

⚠️ **Not yet integrated into SparkScan** - This is intentional for POC!

## How to Use the POC

### Option 1: Direct Usage (Minimal Integration)

Add this method to `SparkScan.java`:

```java
/**
 * POC: Alternative implementation using DistributedScanBuilder.
 * This method can be called instead of the current planScanFiles().
 */
private void planScanFilesUsingDistributedScanBuilder() {
    SparkSession spark = SparkSession.active();
    int numPartitions = 50;
    
    // Create distributed scan builder
    ScanBuilder scanBuilder = new DistributedScanBuilder(
        spark, initialSnapshot, numPartitions);
    
    // Apply filters if any
    if (dataFilters != null && dataFilters.length > 0) {
        for (Filter filter : dataFilters) {
            // TODO: Convert Spark Filter to Kernel Predicate
            // scanBuilder = scanBuilder.withFilter(convertFilter(filter));
        }
    }
    
    // Build scan
    Scan scan = scanBuilder.build();
    
    // Get files using standard Kernel API
    final Engine tableEngine = DefaultEngine.create(hadoopConf);
    final String tablePath = getTablePath();
    
    try (CloseableIterator<FilteredColumnarBatch> scanFileBatches = 
            scan.getScanFiles(tableEngine)) {
        
        while (scanFileBatches.hasNext()) {
            FilteredColumnarBatch batch = scanFileBatches.next();
            
            try (CloseableIterator<Row> addFileRowIter = batch.getRows()) {
                while (addFileRowIter.hasNext()) {
                    Row row = addFileRowIter.next();
                    AddFile addFile = new AddFile(row);
                    
                    PartitionedFile partitionedFile = 
                        PartitionUtils.buildPartitionedFile(
                            addFile, partitionSchema, tablePath, zoneId);
                    
                    totalBytes += addFile.getSize();
                    partitionedFiles.add(partitionedFile);
                }
            }
        }
    } catch (IOException e) {
        throw new RuntimeException(e);
    }
}
```

### Option 2: Replace Existing Implementation

Replace the entire `planScanFiles()` method:

```java
private void planScanFiles() {
    // Choose implementation based on configuration or table size
    boolean useDistributed = spark.conf()
        .getOption("spark.databricks.delta.v2.useDistributedScanBuilder")
        .map(Boolean::parseBoolean)
        .getOrElse(() -> false);
    
    if (useDistributed) {
        planScanFilesUsingDistributedScanBuilder();
    } else {
        planScanFilesOriginal();  // Rename current implementation
    }
}
```

### Option 3: Snapshot-Level Integration (Most Elegant)

Modify how Snapshot creates ScanBuilder:

```java
// In SnapshotImpl or similar
public ScanBuilder getScanBuilder() {
    SparkSession spark = SparkSession.active();
    
    // Decide based on table size or configuration
    boolean useDistributed = shouldUseDistributedScan();
    
    if (useDistributed) {
        return new DistributedScanBuilder(spark, this, 50);
    } else {
        return super.getScanBuilder();  // Kernel's serial
    }
}
```

## Testing the POC

### Step 1: Compile
```bash
cd /Users/xin.huang/oss/delta
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home
build/sbt "project sparkV2" "compile"
```

### Step 2: Add Test Method
Create a simple test in `SparkScan`:

```java
// Temporary test method - remove after POC validation
public void testDistributedScanBuilderPOC() {
    SparkSession spark = SparkSession.active();
    
    // Create distributed scan
    DistributedScanBuilder builder = new DistributedScanBuilder(
        spark, initialSnapshot, 10);
    
    Scan scan = builder.build();
    
    // Count files
    int fileCount = 0;
    Engine engine = DefaultEngine.create(hadoopConf);
    
    try (CloseableIterator<FilteredColumnarBatch> iter = scan.getScanFiles(engine)) {
        while (iter.hasNext()) {
            FilteredColumnarBatch batch = iter.next();
            fileCount += batch.getData().getSize();
        }
    } catch (IOException e) {
        throw new RuntimeException(e);
    }
    
    System.out.println("DistributedScanBuilder found " + fileCount + " files");
}
```

### Step 3: Compare Results
```java
// Compare serial vs distributed
public void compareSerialVsDistributed() {
    // Serial (current)
    long start1 = System.currentTimeMillis();
    planScanFilesOriginal();
    long serial = System.currentTimeMillis() - start1;
    int serialCount = partitionedFiles.size();
    partitionedFiles.clear();
    
    // Distributed (POC)
    long start2 = System.currentTimeMillis();
    planScanFilesUsingDistributedScanBuilder();
    long distributed = System.currentTimeMillis() - start2;
    int distributedCount = partitionedFiles.size();
    
    System.out.println("Serial:      " + serial + "ms, " + serialCount + " files");
    System.out.println("Distributed: " + distributed + "ms, " + distributedCount + " files");
    System.out.println("Speedup:     " + (serial / (double) distributed) + "x");
}
```

## Known Limitations in POC

### 1. Predicate Conversion Not Implemented
```java
@Override
public ScanBuilder withFilter(Predicate predicate) {
    // TODO: Convert Kernel Predicate to Spark Column
    Column sparkFilter = convertPredicateToSparkColumn(predicate);
    // Currently returns null - no filtering
    return this;
}
```

**Workaround**: Filtering happens in `DistributedLogReplayHelper.stateReconstructionV2()` already.

### 2. Complex Types Partially Supported
```java
@Override
public ArrayValue getArray(int ordinal) {
    throw new UnsupportedOperationException("Array not yet supported in POC");
}
```

**Impact**: Only affects tables with array columns. Most Delta tables work fine.

### 3. MapValue Limited Implementation
```java
private static class SparkMapWrapper implements MapValue {
    @Override
    public ArrayValue getKeys() {
        throw new UnsupportedOperationException("getKeys not yet supported");
    }
}
```

**Impact**: Partition values work (most common use case), but full Map iteration not supported.

## Next Steps

### Immediate (Validate POC)
1. ✅ Compile POC code
2. ⏳ Add test method to SparkScan
3. ⏳ Run unit test with small table
4. ⏳ Verify output matches current implementation

### Short Term (Complete POC)
1. Implement predicate conversion (Kernel → Spark)
2. Complete array/map support in SparkRowWrapper
3. Add error handling and logging
4. Performance benchmark

### Long Term (Production)
1. Comprehensive test suite
2. Integration with all Kernel features
3. Documentation
4. Performance optimization

## Architecture Benefits Recap

### Clean Separation
```
SparkScan
  ↓ Uses standard Kernel API
ScanBuilder (interface)
  ↓ Two implementations
├─ KernelScanBuilder (serial)
└─ DistributedScanBuilder (distributed) ← POC
```

### Easy Extension
```java
// Add new features without modifying SparkScan
class DistributedScanBuilder {
    withFilter()      → df.filter()
    withReadSchema()  → df.select()
    withLimit()       → df.limit()      // Easy to add
    withSample()      → df.sample()     // Easy to add
}
```

### Future-Proof
```java
// When Kernel adds new ScanBuilder methods, just implement them
@Override
public ScanBuilder withPushdownAggregation(Aggregation agg) {
    dataFrame = dataFrame.groupBy(...).agg(...);
    return this;
}
```

## Summary

✅ **POC Complete**: All classes implemented  
⚠️ **Not Integrated**: Intentionally kept separate for validation  
📝 **Documentation**: Complete usage examples provided  
🔧 **Next Step**: Add test method and validate

**To integrate**, choose one of the three options above based on your preference:
- **Option 1**: Minimal change, easy to test
- **Option 2**: Toggle between implementations
- **Option 3**: Most elegant, requires Snapshot changes
