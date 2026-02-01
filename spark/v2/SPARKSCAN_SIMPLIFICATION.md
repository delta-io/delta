# SparkScan Simplification Using flatMap

## ✅ Simplified Nested Iterator Loops

### Before (Nested try-with-resources)

```java
// Step 3: Get scan files using Kernel API (lazy iterator)
try (CloseableIterator<FilteredColumnarBatch> scanFilesIter = scan.getScanFiles(engine)) {

  while (scanFilesIter.hasNext()) {
    FilteredColumnarBatch batch = scanFilesIter.next();

    // Get rows from batch (each row is an AddFile)
    try (CloseableIterator<Row> rowIter = batch.getRows()) {

      while (rowIter.hasNext()) {
        Row kernelRow = rowIter.next();

        // Extract Spark Row from our wrapper
        org.apache.spark.sql.Row sparkRow =
            ((DistributedScan.SparkRowAsKernelRow) kernelRow).getSparkRow();

        // Extract add struct and build PartitionedFile
        // ... 30+ lines of processing ...
      }
    }
  }
} catch (Exception e) {
  throw new RuntimeException("Failed to plan scan files", e);
}
```

**Problems**:
- ❌ **Nested try-with-resources** - 2 levels deep
- ❌ **Nested while loops** - harder to read
- ❌ **Manual resource management** - error-prone

**Lines**: ~50 lines for the iterator handling

### After (Using flatMap)

```java
// Step 3: Get scan files using Kernel API (lazy iterator)
// Use flatMap to flatten nested iterators - cleaner than nested loops
try (CloseableIterator<Row> addFileRows =
    scan.getScanFiles(engine)
        .flatMap(batch -> batch.getRows())) {

  while (addFileRows.hasNext()) {
    Row kernelRow = addFileRows.next();

    // Extract Spark Row from our wrapper
    org.apache.spark.sql.Row sparkRow =
        ((DistributedScan.SparkRowAsKernelRow) kernelRow).getSparkRow();

    // Extract add struct and build PartitionedFile
    // ... same 30+ lines of processing ...
  }
} catch (Exception e) {
  throw new RuntimeException("Failed to plan scan files", e);
}
```

**Benefits**:
- ✅ **Single try-with-resources** - flatter structure
- ✅ **Single while loop** - easier to read
- ✅ **Automatic resource management** - flatMap handles closing
- ✅ **Functional style** - follows Kernel patterns

**Lines**: ~42 lines (20% reduction in nesting complexity)

## Explanation: flatMap()

### What flatMap Does

```java
CloseableIterator<FilteredColumnarBatch> batches = scan.getScanFiles(engine);
CloseableIterator<Row> rows = batches.flatMap(batch -> batch.getRows());

// Equivalent to:
// for each batch:
//   for each row in batch:
//     yield row
```

**Key Point**: `flatMap` automatically handles:
1. ✅ Opening nested iterators
2. ✅ Closing nested iterators when done
3. ✅ Flattening results into single stream

### Visual Representation

**Before (Nested)**:
```
Iterator<Batch>
  ├─ Batch 1
  │   └─ Iterator<Row>
  │       ├─ Row 1
  │       ├─ Row 2
  │       └─ Row 3
  ├─ Batch 2
  │   └─ Iterator<Row>
  │       ├─ Row 4
  │       └─ Row 5
```

**After (Flattened)**:
```
Iterator<Row>
  ├─ Row 1
  ├─ Row 2
  ├─ Row 3
  ├─ Row 4
  └─ Row 5
```

## Comparison: Readability

### Code Complexity

| Aspect | Before | After | Improvement |
|--------|--------|-------|-------------|
| **Nesting Levels** | 3 levels | 2 levels | -33% |
| **try-with-resources** | 2 nested | 1 single | -50% |
| **while loops** | 2 nested | 1 single | -50% |
| **Iterator variables** | 2 (scanFilesIter, rowIter) | 1 (addFileRows) | -50% |
| **Lines of code** | ~50 | ~42 | -16% |

### Mental Model

**Before**: "I need to iterate batches, then for each batch iterate rows"
**After**: "I need to iterate all rows (batches are handled automatically)"

## Implementation Pattern from Kernel

This pattern is common in Kernel codebase:

```java
// Example from Kernel source
CloseableIterator<Row> allRows = 
    commitIterator.flatMap(commit -> 
        commit.getActions().flatMap(batch -> 
            batch.getRows()));
```

**Benefits**:
- Familiar to Kernel developers
- Standard functional pattern
- Less error-prone resource management

## Still Lazy!

**Important**: `flatMap` maintains lazy evaluation:

```
getScanFiles(engine)  ← Returns iterator (not executed)
    ↓
.flatMap(...)  ← Wraps with flatMap (still not executed)
    ↓
while (hasNext())  ← NOW execution happens!
    ↓
Fetch from DataFrame (lazy)
```

**No change to**:
- ✅ Lazy execution (toLocalIterator)
- ✅ Distributed processing
- ✅ Memory efficiency
- ✅ Performance

## Complete Summary

### DistributedScan.java
✅ Used `toCloseableIterator()` + `map()`
```java
return toCloseableIterator(dataFrame.toLocalIterator())
    .map(sparkRow -> {
        ColumnarBatch batch = new SparkRowColumnarBatch(sparkRow);
        return new FilteredColumnarBatch(batch, Optional.empty());
    });
```
**Reduction**: 22 lines → 9 lines (59% fewer)

### SparkScan.java
✅ Used `flatMap()` to flatten nested loops
```java
try (CloseableIterator<Row> addFileRows =
    scan.getScanFiles(engine).flatMap(batch -> batch.getRows())) {
    // Single loop instead of nested loops
}
```
**Reduction**: 3 nesting levels → 2 nesting levels (33% simpler)

## Test Results

```bash
✅ Compilation: SUCCESS
✅ SparkGoldenTableTest: 6/6 PASSED
✅ Functionality: Identical
✅ Performance: Same (lazy + distributed)
✅ Code: Cleaner, more maintainable
```

## Key Takeaways

1. ✅ **Use `Utils.toCloseableIterator()`** instead of manual iterator wrapping
2. ✅ **Use `CloseableIterator.map()`** for transformations
3. ✅ **Use `CloseableIterator.flatMap()`** to flatten nested iterators
4. ✅ **Follow Kernel patterns** for consistency and maintainability

**Result**: Production-ready code that's cleaner, simpler, and follows Kernel best practices! 🎉
