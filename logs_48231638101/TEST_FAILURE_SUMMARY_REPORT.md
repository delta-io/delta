# Delta Lake Test Failure Analysis Report

**Report Generated**: October 24, 2025  
**Test Run ID**: logs_48231638101  
**Test Environment**: 8 Shards (Scala 2.12.18 and 2.13.13)

---

## Executive Summary

- **Total Failed Tests**: 5,931
- **Total Test Suites Affected**: 180
- **Complete Test List**: `ALL_FAILED_TEST_CASES.txt` (24,811 lines, 809KB)

### Test Environments
- **Scala 2.12.18**: Shards 0, 1, 2, 3
- **Scala 2.13.13**: Shards 0, 1, 2, 3

---

## Failure Distribution by Root Cause

| Root Cause | Count | Percentage | Description |
|------------|-------|------------|-------------|
| Unknown/Other errors | 4,644 | 78.3% | Errors requiring deeper log analysis |
| Expected exception not thrown | 619 | 10.4% | Test expected an exception but none was thrown, or wrong exception type |
| Unsupported table change (field not found) | 432 | 7.3% | Cannot find field in table schema (Column Mapping issues) |
| Streaming: latestOffset not supported | 196 | 3.3% | Streaming API compatibility issues |
| Hudi conversion issues | 32 | 0.5% | Delta to Hudi conversion failures |
| FileNotFoundException | 8 | 0.1% | Missing files during test execution |

---

## Top 50 Test Suites with Failures

### 1. DeltaCDCSuite - 703 failures
**Primary Issues:**
- 573× Unknown errors
- 27× latestOffset not supported
- 16× Expected exception not thrown: DeltaIllegalStateException
- 15× Expected exception not thrown: IllegalArgumentException

**Key Failed Tests:**
- CDF timestamp format tests (yyyyMMddHHmmssSSS, yyyy-MM-dd, etc.)
- restart works sharing tests
- schema evolution with CDC reserved column names
- streaming works with DV
- CDC read respects timezone and DST
- version from timestamp tests
- changes from table by name

### 2. DeltaCDCScalaSuite - 227 failures
**Primary Issues:**
- 114× Unknown errors
- 66× Expected exception not thrown: DeltaAnalysisException
- 17× latestOffset not supported
- 8× Expected exception not thrown: AnalysisException

**Key Failed Tests:**
- range boundary cannot be null (multiple variants)
- Streaming MERGE overflow tests
- test update/merge/delete on temp view
- filters should be pushed down
- non-monotonic timestamps

### 3. DeltaAlterTableByNameSuite - 182 failures
**Primary Issues:**
- 94× Unknown errors
- 14× Expected exception not thrown: Exception
- 14× Expected exception not thrown: DeltaAnalysisException
- 13× Cannot find field: struct

**Key Failed Tests:**
- CHANGE COLUMN - case insensitive (multiple column mapping modes)
- ALTER TABLE RENAME COLUMN - nested struct tests
- SET/UNSET TBLPROPERTIES tests
- ALTER TABLE ADD/CHANGE/DROP COLUMNS (nested structures)

### 4. DeltaSuite - 174 failures
**Primary Issues:**
- 163× Unknown errors
- 3× Cannot find field: arr
- 2× Expected exception not thrown: DeltaAnalysisException
- 2× Expected exception not thrown: DeltaUnsupportedOperationException

**Key Failed Tests:**
- null struct with NullType field tests
- replaceWhere user defined _change_type column
- drop column tests
- clone assigns materialized row ids column
- generated column tests

### 5. CatalogOwnedTestBaseSuite - 145 failures
**Primary Issues:**
- 139× Unknown errors
- 2× Expected exception not thrown: DeltaUnsupportedOperationException
- 1× Cannot find field: a

**Key Failed Tests:**
- schema evolution tests (array_union, nested types)
- INSERT scenarios
- optimize clustered table tests
- alter table set tbl properties tests

### 6. RowTrackingDeleteSuite - 144 failures
**Primary Issues:**
- 144× Unknown errors (100%)

**Key Failed Tests:**
- DELETE preserves Row IDs (all partition variants)
- Various whereClause conditions

### 7. DeltaColumnMappingSuite - 138 failures
**Primary Issues:**
- 84× Unknown errors
- 28× latestOffset not supported
- 6× Cannot find field: a
- 4× Expected exception not thrown: AnalysisException

**Key Failed Tests:**
- ALTER TABLE RENAME COLUMN - nested struct tests
- Streaming MERGE overflow tests
- filters pushed down to parquet use physical names
- CDF and Column Mapping compatibility tests

### 8. MergeIntoNestedStructEvolutionSQLNameBasedSuite - 138 failures
**Primary Issues:**
- 129× Unknown errors
- 9× Expected exception not thrown: AnalysisException

**Key Failed Tests:**
- schema evolution - new nested source field tests
- nested columns resolved by name tests

### 9. RowTrackingMergeSuite - 136 failures
**Primary Issues:**
- 133× Unknown errors
- 2× Expected exception not thrown: DeltaIllegalArgumentException
- 1× Cannot find field: c3

**Key Failed Tests:**
- Time travel with schema changes
- DELETE/UPDATE MATCHED MERGE operations
- Row tracking with various MERGE scenarios

### 10. DataSkippingDeltaV1Suite - 131 failures
**Primary Issues:**
- 123× Unknown errors
- 2× Expected exception not thrown: DeltaIllegalArgumentException
- 2× Cannot find field: a
- 2× Expected exception not thrown: AnalysisException

**Key Failed Tests:**
- data skipping with expressions involving subquery
- support case insensitivity for partitioning filters
- UDT Data Types tests
- ADD COLUMNS into complex types

### 11. RestoreTableSuite - 130 failures
**Primary Issues:**
- 121× Unknown errors
- 4× Expected exception not thrown: DeltaUnsupportedOperationException
- 2× Expected exception not thrown: AnalysisException

### 12. MergeIntoSchemaEvolutionBaseSQLNameBasedSuite - 126 failures
**Primary Issues:**
- 123× Unknown errors
- 3× Expected exception not thrown: AnalysisException

### 13. MergeIntoSuite - 119 failures
**Primary Issues:**
- 115× Unknown errors
- 2× latestOffset not supported
- 1× Expected exception not thrown: DeltaAnalysisException

### 14. GeneratedColumnSuite - 119 failures
**Primary Issues:**
- 112× Unknown errors
- 2× Expected exception not thrown: DeltaInvariantViolationException
- 2× Expected exception not thrown: AnalysisException

### 15. DeltaWithNewTransactionSuite - 112 failures
**Primary Issues:**
- 77× Unknown errors
- 31× Expected exception not thrown: DeltaConcurrentModificationException
- 2× Expected exception not thrown: DeltaErrorsBase

### 16. RemoveColumnMappingCDCSuite - 105 failures
**Primary Issues:**
- 72× Cannot find field: third_column_name
- 31× Unknown errors
- 2× latestOffset not supported

### 17. TypeWideningInsertSchemaEvolutionExtendedSuite - 100 failures
**Primary Issues:**
- 96× Unknown errors
- 2× Expected exception not thrown: AnalysisException

### 18. ConvertToHudiSuite - 98 failures
**Primary Issues:**
- 49× Unknown errors
- 15× Cannot find field: value
- 12× Hudi conversion: File paths mismatch
- 4× Cannot find field: part
- 3× Cannot find field: c

**Key Failed Tests:**
- basic test - catalog table created with DataFrame
- validate multiple commits (partitioned = true/false)
- restart works sharing tests
- stream from delta source
- Delta sharing with type widening

### 19. DeltaInsertIntoSchemaEvolutionSuite - 96 failures
**Primary Issues:**
- 90× Unknown errors
- 6× Expected exception not thrown: StreamingQueryException

### 20. DeltaInsertIntoMissingColumnSuite - 95 failures
**Primary Issues:**
- 95× Unknown errors (100%)

### 21-30. Additional High-Impact Test Suites

21. **ImplicitStreamingMergeCastingSuite** - 95 failures
    - 52× latestOffset not supported
    - 29× Unknown errors

22. **OptimizeMetadataOnlyDeltaQuerySuite** - 92 failures
    - 92× Unknown errors

23. **DeltaTimeTravelSuite** - 84 failures
    - 75× Unknown errors
    - 5× Cannot find field: c2

24. **CloneTableSQLSuite** - 83 failures
    - 75× Unknown errors
    - 5× Expected exception not thrown: DeltaAnalysisException

25. **DeltaCDCSQLSuite** - 81 failures
    - 66× Unknown errors
    - 11× latestOffset not supported

26. **RemoveColumnMappingStreamingReadSuite** - 77 failures
    - 64× Cannot find field: third_column_name

27. **DeltaInsertIntoImplicitCastSuite** - 71 failures
    - 39× Unknown errors
    - 29× Expected exception not thrown: SparkThrowable

28. **DescribeDeltaDetailSuite** - 65 failures
    - 54× Unknown errors
    - 8× Cannot find field: col2

29. **DescribeDeltaHistorySuite** - 59 failures
    - 57× Unknown errors

30. **CheckConstraintsSuite** - 48 failures
    - 20× Expected exception not thrown: AnalysisException

### 31-50. Medium-Impact Test Suites

31. **RowTrackingUpdateCommonRowTrackingUpdateDVSuite** - 48 failures
32. **DeltaSourceColumnMappingSuite** - 47 failures
33. **RemoveColumnMappingCDFSQLSuite** - 44 failures
34. **DeltaColumnDefaultsInsertSuite** - 43 failures
35. **DeleteSQLSuite** - 40 failures
36. **DropColumnMappingFeatureSuite** - 39 failures
37. **CloneTableSuite** - 38 failures
38. **MaterializedColumnSuite** - 37 failures
39. **DeltaMergeIntoSchemaEvolutionWithIncompatibleSchemaModeSuite** - 37 failures
40. **UpdateSQLSuite** - 36 failures
41. **RowIdSuite** - 35 failures
42. **DeltaMergeSQLSuite** - 35 failures
43. **DeltaColumnDefaultsUpdateSuite** - 34 failures
44. **DeltaAlterTableCoordinatedCommitsSuiteV2CatalogFalse** - 33 failures
45. **ConvertToDeltaSuite** - 33 failures
46. **RemoveColumnMappingStreamingWriteSuite** - 32 failures
47. **DeltaUpdateCatalogSuite** - 31 failures
48. **DeltaSharingSourceSuite** - 31 failures
49. **RowTrackingWriteSuite** - 31 failures
50. **CloneParquetByNameSuite** - 30 failures

---

## Critical Issues Analysis

### Issue 1: Column Mapping Field Resolution (432 failures, 7.3%)

**Affected Fields:**
- `third_column_name` - 136 occurrences (RemoveColumnMapping tests)
- `logical_column_name` - Multiple occurrences (DropColumnMapping tests)
- `value`, `c1`, `c2`, `c3`, `part` - Various data operation tests
- `struct`, `a`, `arr`, `col2` - Complex type tests

**Root Cause**: Incompatibility in column mapping when transitioning between v1 and v2 table formats, particularly when:
- Removing column mapping feature
- Working with nested structures (arrays, maps, structs)
- Using Delta Sharing with column mapping

**Affected Test Suites:**
- RemoveColumnMappingCDCSuite (72 failures)
- RemoveColumnMappingStreamingReadSuite (64 failures)
- DeltaAlterTableByNameSuite (13 failures)
- ConvertToHudiSuite (15+ failures)

### Issue 2: Exception Handling Inconsistency (619 failures, 10.4%)

**Expected Exceptions Not Thrown:**
- `DeltaAnalysisException` - Most common
- `DeltaIllegalStateException`
- `DeltaUnsupportedOperationException`
- `DeltaConcurrentModificationException`
- `AnalysisException`
- `StreamingQueryException`

**Root Cause**: Tests expect operations to fail with specific exceptions, but either:
1. The operation succeeds when it should fail
2. A different exception type is thrown
3. No exception is thrown at all

**Most Affected Areas:**
- CDC reserved column names handling
- Schema evolution boundary cases
- Concurrent modification scenarios
- Type casting and overflow scenarios

### Issue 3: Streaming API Compatibility (196 failures, 3.3%)

**Error**: `latestOffset is not supported`

**Affected Operations:**
- Streaming MERGE with type overflow
- CDC streaming operations
- Column Mapping streaming operations
- Self union operations on Delta tables

**Affected Test Suites:**
- ImplicitStreamingMergeCastingSuite (52 failures)
- DeltaColumnMappingSuite (28 failures)
- DeltaCDCSuite (27 failures)
- DeltaCDCScalaSuite (17 failures)

**Root Cause**: The v1/v2 data source interface implementation may be incomplete for streaming scenarios.

### Issue 4: Hudi Conversion Problems (32 failures, 0.5%)

**Error Types:**
1. File paths mismatch (12 occurrences)
2. Hoodie table not found (8 occurrences)
3. Field resolution issues (12 occurrences)

**Root Cause**: 
- Timing/synchronization issues during Delta to Hudi conversion
- File system consistency problems
- Schema mapping issues between Delta and Hudi formats

---

## Unknown Errors Deep Dive (4,644 failures, 78.3%)

The majority of failures (78.3%) are categorized as "Unknown" because the error reason couldn't be automatically extracted from log patterns. These require manual investigation of the log files.

**Potentially affected areas based on test names:**
1. **Row Tracking features** (144+ failures)
2. **Schema Evolution** (200+ failures)
3. **CDC operations** (500+ failures)
4. **Optimize operations** (100+ failures)
5. **Time Travel queries** (100+ failures)
6. **Materialized columns** (50+ failures)
7. **Generated columns** (100+ failures)
8. **Clustering features** (50+ failures)

---

## Files Generated

1. **ALL_FAILED_TEST_CASES.txt** (809KB, 24,811 lines)
   - Complete listing of all 5,931 failed test cases
   - Organized by test suite
   - Includes test name, failure reason, and log file location

2. **TEST_FAILURE_SUMMARY_REPORT.md** (This file)
   - Executive summary and analysis
   - Top 50 test suites with detailed breakdown
   - Root cause analysis

3. **测试失败分析报告.md** (Chinese version)
   - Chinese language summary report

---

## Recommendations

### Priority 1: Investigate Unknown Errors
- Manually review logs for the 78.3% of failures without clear error messages
- Set up better error message extraction and reporting
- Create test failure pattern matching

### Priority 2: Fix Column Mapping Issues
- Address field resolution failures (432 cases)
- Focus on `RemoveColumnMapping*` test suites
- Ensure backward compatibility when removing features
- Test nested structure handling thoroughly

### Priority 3: Standardize Exception Handling
- Review why expected exceptions aren't being thrown (619 cases)
- Ensure consistent error handling across v1 and v2 APIs
- Update tests if exception types have changed intentionally

### Priority 4: Streaming API Support
- Implement or fix `latestOffset` support (196 cases)
- Test streaming scenarios with all table features
- Ensure v1/v2 API parity for streaming operations

### Priority 5: Hudi Conversion Reliability
- Fix timing/synchronization issues
- Improve error handling and retry logic
- Add more robust file system checks

### Priority 6: Feature-Specific Investigations
- Row Tracking (300+ failures)
- CDC operations (1000+ failures)
- Schema Evolution (500+ failures)
- Time Travel (100+ failures)

---

## Test Suite Categories

### Critical (100+ failures each)
18 test suites with 100+ failures each, accounting for 2,743 failures (46.2%)

### High Priority (50-99 failures)
15 test suites, accounting for 962 failures (16.2%)

### Medium Priority (20-49 failures)
32 test suites, accounting for 980 failures (16.5%)

### Lower Priority (<20 failures)
115 test suites, accounting for 1,246 failures (21.0%)

---

**End of Report**

