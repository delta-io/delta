/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.sparkuctest;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies DSv2 streaming reads match batch reads across table creation/mutation variants.
 *
 * <p>For <b>append-only</b> variants, streaming output equals the batch table state. For
 * <b>data-modifying</b> variants (UPDATE/DELETE/MERGE/OVERWRITE), streaming output differs because
 * Delta rewrites files: e.g., DELETE removes the old file and creates a new one without the deleted
 * row, so streaming sees rows from both files. These variants use {@code streamingReadOptions}
 * (ignoreDeletes/ignoreChanges) and only assert the stream completes without error.
 */
public class UCDeltaStreamingTableVariantTest extends UCDeltaTableIntegrationBaseTest {

  private static final long STREAMING_TIMEOUT_MS = 60_000L;
  @TempDir private Path tempDir;
  private int checkpointCount;

  private static class TableVariant {
    final String name;
    final String schema;
    final String partitionCols;
    final String tableProperties;
    final String createTableSql;
    final List<String> setupSqls;
    final List<String> incrementalSqls;
    final Map<String, String> streamingReadOptions;
    final Map<String, String> sparkConfOverrides;

    /** Append-only variant using default DDL. */
    TableVariant(
        String name,
        String schema,
        String partitionCols,
        String tableProperties,
        List<String> setupSqls,
        List<String> incrementalSqls) {
      this(
          name,
          schema,
          partitionCols,
          tableProperties,
          null,
          setupSqls,
          incrementalSqls,
          Collections.emptyMap());
    }

    /** Full constructor for custom DDL and/or streaming read options. */
    TableVariant(
        String name,
        String schema,
        String partitionCols,
        String tableProperties,
        String createTableSql,
        List<String> setupSqls,
        List<String> incrementalSqls,
        Map<String, String> streamingReadOptions) {
      this(
          name,
          schema,
          partitionCols,
          tableProperties,
          createTableSql,
          setupSqls,
          incrementalSqls,
          streamingReadOptions,
          Collections.emptyMap());
    }

    /** Full constructor with Spark config overrides (reset via unset after each test). */
    TableVariant(
        String name,
        String schema,
        String partitionCols,
        String tableProperties,
        String createTableSql,
        List<String> setupSqls,
        List<String> incrementalSqls,
        Map<String, String> streamingReadOptions,
        Map<String, String> sparkConfOverrides) {
      this.name = name;
      this.schema = schema;
      this.partitionCols = partitionCols;
      this.tableProperties = tableProperties;
      this.createTableSql = createTableSql;
      this.setupSqls = setupSqls;
      this.incrementalSqls = incrementalSqls;
      this.streamingReadOptions = streamingReadOptions;
      this.sparkConfOverrides = sparkConfOverrides;
    }

    boolean isAppendOnly() {
      return streamingReadOptions.isEmpty();
    }
  }

  // Add new variants here. Each is tested with SNAPSHOT + INCREMENTAL x EXTERNAL + MANAGED.
  private static final List<TableVariant> TABLE_VARIANTS =
      List.of(

          // -- Create table, INSERT, then stream --
          new TableVariant(
              /* name */ "SimpleCreateTable",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* setupSqls */ List.of("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')"),
              /* incrementalSqls */ List.of("INSERT INTO %s VALUES (4, 'd'), (5, 'e')")),

          // -- Create table with PARTITIONED BY, INSERT across partitions, then stream --
          new TableVariant(
              /* name */ "PartitionedTable",
              /* schema */ "id INT, value STRING, part STRING",
              /* partitionCols */ "part",
              /* tableProperties */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')"),
              /* incrementalSqls */ List.of("INSERT INTO %s VALUES (4, 'd', 'y'), (5, 'e', 'z')")),

          // -- Create table, INSERT 3 separate commits, then stream --
          new TableVariant(
              /* name */ "MultipleInserts",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, 'a')",
                  "INSERT INTO %s VALUES (2, 'b'), (3, 'c')", "INSERT INTO %s VALUES (4, 'd')"),
              /* incrementalSqls */ List.of(
                  "INSERT INTO %s VALUES (5, 'e'), (6, 'f')", "INSERT INTO %s VALUES (7, 'g')")),

          // -- Create table with CLUSTER BY, INSERT, then stream --
          new TableVariant(
              /* name */ "ClusteredTable",
              /* schema */ "id INT, value STRING, category STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* createTableSql */ "CREATE TABLE %s (id INT, value STRING, category STRING)"
                  + " USING DELTA CLUSTER BY (category) %s",
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1,'a','cat1'), (2,'b','cat2'), (3,'c','cat1')"),
              /* incrementalSqls */ List.of("INSERT INTO %s VALUES (4,'d','cat2'), (5,'e','cat3')"),
              /* streamReadOptions */ Collections.emptyMap()),

          // -- Create table with various data types, INSERT, then stream --
          new TableVariant(
              /* name */ "VariousDataTypes",
              /* schema */ "id INT, b BOOLEAN, d DOUBLE, dec DECIMAL(10,2),"
                  + " dt DATE, ts TIMESTAMP, s STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES"
                      + " (1, true, 1.5, 10.25, DATE'2025-01-01', TIMESTAMP'2025-01-01 12:00:00', 'hello'),"
                      + " (2, false, 2.7, 20.50, DATE'2025-06-15', TIMESTAMP'2025-06-15 18:30:00', 'world')"),
              /* incrementalSqls */ List.of(
                  "INSERT INTO %s VALUES"
                      + " (3, true, 3.14, 30.00, DATE'2025-12-31', TIMESTAMP'2025-12-31 23:59:59', 'end')")),

          // -- Create table, INSERT, INSERT OVERWRITE, then stream with ignoreChanges --
          new TableVariant(
              /* name */ "InsertOverwrite",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* createTableSql */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1,'a'), (2,'b')",
                  "INSERT OVERWRITE %s VALUES (3,'c'), (4,'d')"),
              /* incrementalSqls */ List.of(),
              /* streamReadOptions */ Map.of("ignoreChanges", "true")),

          // -- Create table, INSERT, UPDATE one row, then stream with ignoreChanges --
          new TableVariant(
              /* name */ "AfterUpdate",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* createTableSql */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1,'a'), (2,'b'), (3,'c')",
                  "UPDATE %s SET value = 'z' WHERE id = 1"),
              /* incrementalSqls */ List.of(),
              /* streamReadOptions */ Map.of("ignoreChanges", "true")),

          // -- Create table, INSERT, DELETE one row, then stream with ignoreDeletes --
          new TableVariant(
              /* name */ "AfterDelete",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* createTableSql */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1,'a'), (2,'b'), (3,'c')", "DELETE FROM %s WHERE id = 1"),
              /* incrementalSqls */ List.of(),
              /* streamReadOptions */ Map.of("ignoreDeletes", "true")),

          // -- Create table, INSERT, MERGE (update + insert), then stream with ignoreChanges --
          new TableVariant(
              /* name */ "AfterMerge",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* createTableSql */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1,'a'), (2,'b'), (3,'c')",
                  "MERGE INTO %s t USING (SELECT 1 AS id, 'merged' AS value) s"
                      + " ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value"
                      + " WHEN NOT MATCHED THEN INSERT *"),
              /* incrementalSqls */ List.of(),
              /* streamReadOptions */ Map.of("ignoreChanges", "true")),

          // -- Create table, INSERT v1, INSERT v2, RESTORE to v1, then stream with ignoreChanges --
          new TableVariant(
              /* name */ "AfterRestore",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* createTableSql */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1,'a'), (2,'b')",
                  "INSERT INTO %s VALUES (3,'c')", "RESTORE %s TO VERSION AS OF 1"),
              /* incrementalSqls */ List.of(),
              /* streamReadOptions */ Map.of("ignoreChanges", "true")),

          // -- Create table, INSERT, DPO on partitioned table, then stream with ignoreChanges --
          new TableVariant(
              /* name */ "DynamicPartitionOverwrite",
              /* schema */ "id INT, value STRING, part STRING",
              /* partitionCols */ "part",
              /* tableProperties */ null,
              /* createTableSql */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1,'a','x'), (2,'b','y')",
                  "INSERT OVERWRITE %s VALUES (10,'z','x')"),
              /* incrementalSqls */ List.of(),
              /* streamReadOptions */ Map.of("ignoreChanges", "true"),
              /* sparkConfOverrides */ Map.of(
                  "spark.sql.sources.partitionOverwriteMode", "dynamic")),

          // -- Create table, INSERT with duplicate first-column values, then stream --
          new TableVariant(
              /* name */ "DuplicateFirstColumnValues",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1,'a'), (1,'b'), (1,'c'), (2,'x'), (2,'y')"),
              /* incrementalSqls */ List.of("INSERT INTO %s VALUES (1,'d'), (3,'z')")),

          // -- Create table, INSERT with NULLs in various columns including first, then stream --
          // BUG: MANAGED tables fail with NPE in OnHeapColumnVector.putNotNulls —
          // the vectorized Parquet reader's "this.nulls" byte array is uninitialized
          // when streaming from catalog-managed tables containing NULL values.
          // EXTERNAL tables pass. Both SNAPSHOT and INCREMENTAL modes affected.
          new TableVariant(
              /* name */ "NullsInColumns",
              /* schema */ "id INT, value STRING, opt_int INT",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, null, null), (2, 'b', null), (null, null, null)"),
              /* incrementalSqls */ List.of("INSERT INTO %s VALUES (3, 'c', 3), (null, 'x', 1)")),

          // -- Create table with ARRAY, MAP, STRUCT columns, INSERT, then stream --
          // BUG: MANAGED tables fail with NPE in SparkMicroBatchStream when processing
          // complex types (ARRAY/MAP/STRUCT) in INCREMENTAL mode. SNAPSHOT mode and
          // EXTERNAL tables pass.
          new TableVariant(
              /* name */ "ComplexTypes",
              /* schema */ "id INT, arr ARRAY<INT>, m MAP<STRING,INT>,"
                  + " s STRUCT<a:INT,b:STRING>",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES"
                      + " (1, array(1,2), map('k',1), named_struct('a',1,'b','x')),"
                      + " (2, array(3), map('k1',2,'k2',3), named_struct('a',2,'b','y'))"),
              /* incrementalSqls */ List.of(
                  "INSERT INTO %s VALUES" + " (3, array(), map(), named_struct('a',3,'b','z'))")),

          // -- Create empty table (0 rows), then stream --
          new TableVariant(
              /* name */ "EmptyTable",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* setupSqls */ List.of(),
              /* incrementalSqls */ List.of("INSERT INTO %s VALUES (1, 'first'), (2, 'second')")),

          // -- Create table, INSERT, DELETE ALL rows, INSERT again, then stream --
          new TableVariant(
              /* name */ "DeleteAllThenInsert",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* createTableSql */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1,'a'), (2,'b')",
                  "DELETE FROM %s WHERE true", "INSERT INTO %s VALUES (3,'c')"),
              /* incrementalSqls */ List.of(),
              /* streamReadOptions */ Map.of("ignoreDeletes", "true", "ignoreChanges", "true")),

          // -- Create table, INSERT, MERGE with all 3 clauses (update+delete+insert), stream --
          new TableVariant(
              /* name */ "MergeAllBranches",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* createTableSql */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1,'a'), (2,'b'), (3,'c')",
                  "MERGE INTO %s t USING (SELECT 1 AS id, 'updated' AS value"
                      + " UNION ALL SELECT 2, 'del'"
                      + " UNION ALL SELECT 4, 'new') s ON t.id = s.id"
                      + " WHEN MATCHED AND s.value = 'del' THEN DELETE"
                      + " WHEN MATCHED THEN UPDATE SET value = s.value"
                      + " WHEN NOT MATCHED THEN INSERT *"),
              /* incrementalSqls */ List.of(),
              /* streamReadOptions */ Map.of("ignoreChanges", "true")),

          // -- Create table partitioned by INT column (not STRING), INSERT, then stream --
          new TableVariant(
              /* name */ "IntPartitionColumn",
              /* schema */ "id INT, value STRING, part INT",
              /* partitionCols */ "part",
              /* tableProperties */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1,'a',100), (2,'b',200), (3,'c',100)"),
              /* incrementalSqls */ List.of("INSERT INTO %s VALUES (4,'d',200), (5,'e',300)")),

          // -- Create table with special characters and edge-case strings, then stream --
          new TableVariant(
              /* name */ "SpecialCharStrings",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, 'hello, world'), (2, 'it''s'), (3, '')"),
              /* incrementalSqls */ List.of("INSERT INTO %s VALUES (4, '   '), (5, 'line1')")),

          // -- Create table with column mapping mode=name, partitioned, then stream --
          // Column mapping changes physical column names in Parquet (e.g., col-abc123 instead of
          // id). The DSv2 streaming reader must resolve physical->logical names via
          // ProtocolMetadataAdapterV2. Never tested in UC before; partitions stress partition
          // column name resolution in PartitionUtils.getPartitionRow.
          new TableVariant(
              /* name */ "ColumnMappingName",
              /* schema */ "id INT, value STRING, part STRING",
              /* partitionCols */ "part",
              /* tableProperties */ "'delta.columnMapping.mode'='name'",
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')"),
              /* incrementalSqls */ List.of("INSERT INTO %s VALUES (4, 'd', 'y'), (5, 'e', 'z')")),

          // -- Create table with BOOLEAN columns and interleaved NULLs, then stream --
          // BUG: MANAGED tables fail with NPE in OnHeapColumnVector — same root cause as
          // NullsInColumns (this.nulls byte array uninitialized) but through the BOOLEAN
          // bit-packing path. Confirms the vectorized reader null bug affects multiple types.
          // EXTERNAL tables pass. Both SNAPSHOT and INCREMENTAL modes affected.
          new TableVariant(
              /* name */ "BooleanNulls",
              /* schema */ "id INT, flag BOOLEAN, val INT",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES"
                      + " (1, true, 10), (2, null, 20), (3, false, null), (4, null, null)"),
              /* incrementalSqls */ List.of(
                  "INSERT INTO %s VALUES (5, true, null), (6, null, 60)")),

          // -- Create partitioned table with NULL values in the partition column, then stream --
          // Partition NULLs create __HIVE_DEFAULT_PARTITION__ directory entries. The streaming
          // reader's partition value resolution (PartitionUtils.getPartitionRow) must handle
          // NULL partition values correctly for both EXTERNAL and MANAGED tables.
          new TableVariant(
              /* name */ "NullPartitionValues",
              /* schema */ "id INT, value STRING, part STRING",
              /* partitionCols */ "part",
              /* tableProperties */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES"
                      + " (1, 'a', 'x'), (2, 'b', null), (3, 'c', 'x'), (4, 'd', null)"),
              /* incrementalSqls */ List.of("INSERT INTO %s VALUES (5, 'e', 'y'), (6, 'f', null)")),

          // -- Create table with deletion vectors enabled, INSERT, DELETE, then stream --
          // MANAGED tables have DVs by default; this explicitly enables DVs on EXTERNAL too.
          // DELETE with DVs keeps the original file + a DV bitmap (vs copy-on-write which
          // removes old file and adds new). Tests the DV streaming read path in
          // DeltaParquetFileFormatV2 via deletion vector row filtering.
          new TableVariant(
              /* name */ "DeletionVectorsDelete",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ "'delta.enableDeletionVectors'='true'",
              /* createTableSql */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1,'a'), (2,'b'), (3,'c')", "DELETE FROM %s WHERE id = 2"),
              /* incrementalSqls */ List.of(),
              /* streamReadOptions */ Map.of("ignoreDeletes", "true")),

          // -- Create table via CTAS (CREATE TABLE AS SELECT), then stream --
          // CTAS uses a different write path than CREATE + INSERT: table metadata is derived
          // from the SELECT schema rather than explicit DDL. Tests whether streaming reads
          // work correctly when the table was created via CTAS.
          new TableVariant(
              /* name */ "CTAS",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* createTableSql */ "CREATE TABLE %s USING DELTA %s"
                  + " AS SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(id, value)",
              /* setupSqls */ List.of(),
              /* incrementalSqls */ List.of("INSERT INTO %s VALUES (4, 'd'), (5, 'e')"),
              /* streamReadOptions */ Collections.emptyMap()),

          // -- Create table with two partition columns of different types, then stream --
          // Multiple partition columns exercise per-column partition value injection in
          // PartitionUtils.getPartitionRow. Tests correct handling of mixed-type partition
          // keys across EXTERNAL and MANAGED tables.
          new TableVariant(
              /* name */ "MultiPartitionColumns",
              /* schema */ "id INT, value STRING, part1 STRING, part2 INT",
              /* partitionCols */ "part1, part2",
              /* tableProperties */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES"
                      + " (1, 'a', 'x', 100), (2, 'b', 'y', 200), (3, 'c', 'x', 200)"),
              /* incrementalSqls */ List.of(
                  "INSERT INTO %s VALUES (4, 'd', 'y', 100), (5, 'e', 'z', 300)")),

          // -- Create partitioned table with special characters in partition values, then stream --
          // Partition values with spaces, '=', '#' are URL-encoded in file paths (e.g.,
          // part=hello%20world/). The streaming reader must correctly decode: addFile.getPath()
          // -> new Path(tablePath, path) -> SparkPath.fromUrlString(). Any double-encoding
          // or decode mismatch causes FileNotFoundException or corrupt data.
          new TableVariant(
              /* name */ "SpecialCharsInPartitionValue",
              /* schema */ "id INT, value STRING, part STRING",
              /* partitionCols */ "part",
              /* tableProperties */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES"
                      + " (1, 'a', 'hello world'), (2, 'b', 'a=b'), (3, 'c', 'x#y')"),
              /* incrementalSqls */ List.of(
                  "INSERT INTO %s VALUES (4, 'd', 'hello world'), (5, 'e', 'p(q)')")),

          // -- Create partitioned table with empty string partition value, then stream --
          // Empty string '' is a valid partition value distinct from NULL. The Delta log stores
          // it as "" in the partitionValues map. The streaming reader's partition value
          // pipeline (PartitionUtils.getPartitionRow -> castPartValueToDesiredType) must not
          // conflate '' with NULL. Complements NullPartitionValues.
          new TableVariant(
              /* name */ "EmptyStringPartitionValue",
              /* schema */ "id INT, value STRING, part STRING",
              /* partitionCols */ "part",
              /* tableProperties */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, 'a', ''), (2, 'b', 'x'), (3, 'c', '')"),
              /* incrementalSqls */ List.of("INSERT INTO %s VALUES (4, 'd', ''), (5, 'e', 'y')")),

          // -- Create table with column mapping mode=id (not name), then stream --
          // mode=id uses Parquet field IDs for column resolution instead of physical names.
          // The existing ColumnMappingName variant tests mode=name; mode=id is entirely
          // untested and exercises a fundamentally different Parquet column matching strategy
          // via parquet.field.id metadata.
          new TableVariant(
              /* name */ "ColumnMappingId",
              /* schema */ "id INT, value STRING, part STRING",
              /* partitionCols */ "part",
              /* tableProperties */ "'delta.columnMapping.mode'='id'",
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')"),
              /* incrementalSqls */ List.of("INSERT INTO %s VALUES (4, 'd', 'y'), (5, 'e', 'z')")),

          // -- Create partitioned table with DVs enabled, INSERT, DELETE, then stream --
          // DV column index computation in DeletionVectorSchemaContext interacts with
          // partition columns in the row layout. The DV column is appended after data columns
          // but before partition columns — if the index is wrong, valid rows get filtered or
          // deleted rows leak through.
          new TableVariant(
              /* name */ "DeletionVectorsPartitioned",
              /* schema */ "id INT, value STRING, part STRING",
              /* partitionCols */ "part",
              /* tableProperties */ "'delta.enableDeletionVectors'='true'",
              /* createTableSql */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1,'a','x'), (2,'b','x'), (3,'c','y'), (4,'d','y')",
                  "DELETE FROM %s WHERE id = 2"),
              /* incrementalSqls */ List.of(),
              /* streamReadOptions */ Map.of("ignoreDeletes", "true")),

          // -- Column mapping + DVs + partitions combined, then stream --
          // Tests interaction of 3 features: column mapping renames physical column names,
          // DV appends __delta_internal_is_row_deleted, and partitions inject partition values.
          // If CM rename accidentally touches the DV column (which has no physical name
          // metadata), schema misalignment occurs.
          new TableVariant(
              /* name */ "PartitionedColumnMappingNameDV",
              /* schema */ "id INT, value STRING, part STRING",
              /* partitionCols */ "part",
              /* tableProperties */ "'delta.columnMapping.mode'='name',"
                  + " 'delta.enableDeletionVectors'='true'",
              /* createTableSql */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1,'a','x'), (2,'b','y'), (3,'c','x'), (4,'d','y')",
                  "DELETE FROM %s WHERE id = 2"),
              /* incrementalSqls */ List.of(),
              /* streamReadOptions */ Map.of("ignoreDeletes", "true")));

  // NOTE: The following variants are not yet supported in UC OSS and are omitted:
  // - IdentityColumn, GeneratedColumns, DefaultColumns: UC doesn't support these DDL features
  // - PK/FK: UC doesn't support table constraints
  // - TRUNCATE: UC tables don't support TRUNCATE
  // - OPTIMIZE: blocked for catalog-managed tables
  // - VACUUM: blocked for catalog-managed tables, retention check on external
  // - ALTER TABLE: not supported in UC yet
  // - ANALYZE: not supported for v2 tables
  // - CLONE (shallow/deep): shallow leaks tableId property, deep not in OSS syntax
  // - COPY INTO: needs external file source (complex setup)
  // - CREATE OR REPLACE: works for MANAGED only (blocked for EXTERNAL)
  // - REPLACE WHERE: works for MANAGED only via DataFrame API (not SQL)
  // Add these back when UC adds support for the underlying operations.

  @TestFactory
  Stream<DynamicContainer> streamingTableVariants() {
    return TABLE_VARIANTS.stream()
        .map(
            variant -> {
              List<DynamicTest> tests = new ArrayList<>();
              for (TableType tableType : ALL_TABLE_TYPES) {
                tests.add(
                    DynamicTest.dynamicTest(
                        variant.name + " / SNAPSHOT / " + tableType,
                        () -> runSnapshotTest(variant, tableType)));
                if (!variant.incrementalSqls.isEmpty()) {
                  tests.add(
                      DynamicTest.dynamicTest(
                          variant.name + " / INCREMENTAL / " + tableType,
                          () -> runIncrementalTest(variant, tableType)));
                }
              }
              return DynamicContainer.dynamicContainer(variant.name, tests);
            });
  }

  /**
   * Snapshot test: runs all setupSqls to populate the table, then starts a single streaming read
   * with {@link Trigger#AvailableNow()} (processes all existing data and stops). For append-only
   * variants, asserts streaming output matches batch. For data-modifying variants, asserts the
   * stream completes without error and produces rows (streaming != batch due to file rewrites — see
   * class javadoc).
   */
  private void runSnapshotTest(TableVariant v, TableType tableType) throws Exception {
    withTable(
        v,
        tableType,
        fullName -> {
          v.sparkConfOverrides.forEach((k, val) -> spark().conf().set(k, val));
          try {
            // 1. Run all setup SQL to bring the table to the desired state.
            for (String s : v.setupSqls) sql(s, fullName);

            // 2. Stream with AvailableNow: processes all Delta log entries, then stops.
            String qn = "snap_" + UUID.randomUUID().toString().replace("-", "");
            DataStreamReader reader = spark().readStream().format("delta");
            v.streamingReadOptions.forEach(reader::option);
            reader
                .table(fullName)
                .writeStream()
                .format("memory")
                .queryName(qn)
                .outputMode("append")
                .trigger(Trigger.AvailableNow())
                .option("checkpointLocation", checkpoint())
                .start()
                .awaitTermination(STREAMING_TIMEOUT_MS);

            // 3. Verify output.
            if (v.isAppendOnly()) {
              assertStreamingEqualsBatch(qn, fullName);
            } else {
              // Data-modifying: streaming output has duplicates from file rewrites (see class
              // javadoc). Streaming is a superset of batch — every current batch row must
              // appear in the streaming output.
              List<List<String>> streaming = sql("SELECT * FROM %s", qn);
              List<List<String>> batch = sql("SELECT * FROM %s", fullName);
              assertThat(streaming)
                  .as("Streaming should contain all batch rows")
                  .containsAll(batch);
            }
          } finally {
            v.sparkConfOverrides.keySet().forEach(k -> spark().conf().unset(k));
          }
        });
  }

  /**
   * Incremental test: runs setupSqls, starts a continuous streaming query, then feeds additional
   * data round-by-round via incrementalSqls. After each round, calls {@link
   * StreamingQuery#processAllAvailable()} and verifies the accumulated streaming output matches the
   * current batch table state. Tests that streaming correctly picks up new commits as they arrive.
   */
  private void runIncrementalTest(TableVariant v, TableType tableType) throws Exception {
    withTable(
        v,
        tableType,
        fullName -> {
          v.sparkConfOverrides.forEach((k, val) -> spark().conf().set(k, val));
          try {
            // 1. Run all setup SQL to bring the table to the initial state.
            for (String s : v.setupSqls) sql(s, fullName);

            // 2. Start a continuous streaming query (no trigger = runs until stopped).
            String qn = "incr_" + UUID.randomUUID().toString().replace("-", "");
            DataStreamReader reader = spark().readStream().format("delta");
            v.streamingReadOptions.forEach(reader::option);
            StreamingQuery query =
                reader
                    .table(fullName)
                    .writeStream()
                    .format("memory")
                    .queryName(qn)
                    .outputMode("append")
                    .option("checkpointLocation", checkpoint())
                    .start();
            try {
              // 3. Process initial data and verify.
              query.processAllAvailable();
              assertStreamingEqualsBatch(qn, fullName);

              // 4. Feed incremental data round-by-round, verifying after each.
              for (String incrSql : v.incrementalSqls) {
                sql(incrSql, fullName);
                query.processAllAvailable();
                assertStreamingEqualsBatch(qn, fullName);
              }
            } finally {
              query.stop();
            }
          } finally {
            v.sparkConfOverrides.keySet().forEach(k -> spark().conf().unset(k));
          }
        });
  }

  /**
   * Creates a table for the given variant and table type, runs the test, then drops the table. Uses
   * {@link #withNewTable} for standard DDL, or executes custom SQL for variants that need DDL not
   * expressible through withNewTable (e.g., CLUSTER BY). The custom SQL format string has two %s
   * placeholders: table name and a suffix for TBLPROPERTIES + LOCATION.
   */
  private void withTable(TableVariant v, TableType tableType, TestCode testCode) throws Exception {
    if (v.createTableSql != null) {
      String fullName =
          fullTableName("sv_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12));
      String tblProps =
          tableType == TableType.MANAGED
              ? "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')"
              : "";
      if (tableType == TableType.EXTERNAL) {
        withTempDir(
            dir -> {
              String loc = "LOCATION '" + new org.apache.hadoop.fs.Path(dir, "data") + "'";
              sql(String.format(v.createTableSql, fullName, tblProps + " " + loc));
              try {
                testCode.run(fullName);
              } finally {
                sql("DROP TABLE IF EXISTS %s", fullName);
              }
            });
        return;
      }
      sql(String.format(v.createTableSql, fullName, tblProps));
      try {
        testCode.run(fullName);
      } finally {
        sql("DROP TABLE IF EXISTS %s", fullName);
      }
    } else {
      String name = "sv_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
      withNewTable(name, v.schema, v.partitionCols, tableType, v.tableProperties, testCode);
    }
  }

  /**
   * Asserts streaming memory sink has same rows as batch SELECT *. Sorts both result sets in Java
   * by all columns to avoid non-determinism when the first column has duplicate values.
   */
  private void assertStreamingEqualsBatch(String queryName, String tableName) {
    List<List<String>> streaming = sorted(sql("SELECT * FROM %s", queryName));
    List<List<String>> batch = sorted(sql("SELECT * FROM %s", tableName));
    assertThat(streaming).as("Streaming should match batch for %s", tableName).isEqualTo(batch);
  }

  /** Returns a copy of the rows sorted lexicographically by all columns. */
  private static List<List<String>> sorted(List<List<String>> rows) {
    List<List<String>> copy = new ArrayList<>(rows);
    copy.sort(
        (a, b) -> {
          for (int i = 0; i < Math.min(a.size(), b.size()); i++) {
            int c = String.valueOf(a.get(i)).compareTo(String.valueOf(b.get(i)));
            if (c != 0) return c;
          }
          return Integer.compare(a.size(), b.size());
        });
    return copy;
  }

  /**
   * Returns a fresh checkpoint directory. Each call creates a unique subdirectory under @TempDir.
   */
  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  // ---- Lifecycle tests (not TableVariant-based) ----

  /**
   * Tests that a stream correctly handles RESTORE after checkpoint. Scenario: stream processes
   * versions 1+2, checkpoints. Then RESTORE reverts to version 1 (creating version 3). The resumed
   * stream must handle the RESTORE commit (which has RemoveFile + AddFile with dataChange=true)
   * without crashing. New data inserted after RESTORE should also be picked up.
   */
  @TestAllTableTypes
  public void testRestorePastCheckpoint(TableType tableType) throws Exception {
    withNewTable(
        "restore_ck_src",
        "id INT, value STRING",
        tableType,
        srcName ->
            withNewTable(
                "restore_ck_sink",
                "id INT, value STRING",
                tableType,
                sinkName -> {
                  // Phase 1: Two inserts, then stream everything to Delta sink.
                  sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", srcName);
                  sql("INSERT INTO %s VALUES (4, 'd'), (5, 'e')", srcName);

                  String ck = checkpoint();
                  spark()
                      .readStream()
                      .format("delta")
                      .option("ignoreChanges", "true")
                      .table(srcName)
                      .writeStream()
                      .format("delta")
                      .outputMode("append")
                      .trigger(Trigger.AvailableNow())
                      .option("checkpointLocation", ck)
                      .toTable(sinkName)
                      .awaitTermination(STREAMING_TIMEOUT_MS);
                  check(
                      sinkName,
                      List.of(
                          row("1", "a"),
                          row("2", "b"),
                          row("3", "c"),
                          row("4", "d"),
                          row("5", "e")));

                  // Phase 2: RESTORE source to version 1 (first INSERT only), then add data.
                  sql("RESTORE %s TO VERSION AS OF 1", srcName);
                  sql("INSERT INTO %s VALUES (6, 'f')", srcName);

                  // Phase 3: Resume stream from same checkpoint. Processes RESTORE commit
                  // (version 3) and new INSERT (version 4). With ignoreChanges, the stream
                  // should handle the RESTORE's RemoveFile + AddFile without crashing.
                  spark()
                      .readStream()
                      .format("delta")
                      .option("ignoreChanges", "true")
                      .table(srcName)
                      .writeStream()
                      .format("delta")
                      .outputMode("append")
                      .trigger(Trigger.AvailableNow())
                      .option("checkpointLocation", ck)
                      .toTable(sinkName)
                      .awaitTermination(STREAMING_TIMEOUT_MS);

                  // The stream must complete without error. The new INSERT (6,'f') must
                  // appear in the sink. RESTORE may also re-add already-seen rows.
                  List<List<String>> sinkData = sql("SELECT * FROM %s", sinkName);
                  assertThat(sinkData)
                      .as("Sink after RESTORE should contain the new INSERT")
                      .anySatisfy(row -> assertThat(row).contains("6"));
                }));
  }

  /**
   * Tests that two concurrent streaming queries reading the same table with different checkpoints
   * both get consistent, complete views of the data.
   */
  @TestAllTableTypes
  public void testConcurrentStreams(TableType tableType) throws Exception {
    withNewTable(
        "concurrent_stream_test",
        "id INT, value STRING",
        tableType,
        fullName -> {
          sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", fullName);

          String ck1 = checkpoint();
          String ck2 = checkpoint();
          String qn1 =
              "concurrent1_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
          String qn2 =
              "concurrent2_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);

          // Start two concurrent continuous streams.
          StreamingQuery q1 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(fullName)
                  .writeStream()
                  .format("memory")
                  .queryName(qn1)
                  .outputMode("append")
                  .option("checkpointLocation", ck1)
                  .start();
          StreamingQuery q2 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(fullName)
                  .writeStream()
                  .format("memory")
                  .queryName(qn2)
                  .outputMode("append")
                  .option("checkpointLocation", ck2)
                  .start();
          try {
            q1.processAllAvailable();
            q2.processAllAvailable();

            // Both should see the same initial data.
            List<List<String>> r1 = sorted(sql("SELECT * FROM %s", qn1));
            List<List<String>> r2 = sorted(sql("SELECT * FROM %s", qn2));
            assertThat(r1).as("Stream 1 initial data").isEqualTo(r2);
            assertThat(r1).hasSize(3);

            // Insert more data — both should pick it up.
            sql("INSERT INTO %s VALUES (4, 'd'), (5, 'e')", fullName);
            q1.processAllAvailable();
            q2.processAllAvailable();

            r1 = sorted(sql("SELECT * FROM %s", qn1));
            r2 = sorted(sql("SELECT * FROM %s", qn2));
            assertThat(r1).as("Stream 1 after INSERT").isEqualTo(r2);
            assertThat(r1).hasSize(5);
          } finally {
            q1.stop();
            q2.stop();
          }
        });
  }
}
