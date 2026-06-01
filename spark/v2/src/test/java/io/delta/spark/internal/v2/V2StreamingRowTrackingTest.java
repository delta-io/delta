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
package io.delta.spark.internal.v2;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.delta.DeltaLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Option;

/**
 * Regression tests for DSv2 streaming reads that project {@code _metadata} row-tracking fields.
 *
 * <p>Streaming planning never invokes {@code SupportsPushDownRequiredColumns.pruneColumns} (Spark
 * TODO in {@code MicroBatchExecution.scala}). Without compensation in the scan builder, the reader
 * factory emits a 2-column batch while the analyzer-declared output has 3 columns including {@code
 * _metadata}, so codegen reading ordinal 2 crashes with {@code ArrayIndexOutOfBoundsException}.
 *
 * <p>These tests lock the streaming path against that regression for {@code _metadata.row_id},
 * {@code _metadata.row_commit_version}, and the DV + row-tracking interaction.
 */
public class V2StreamingRowTrackingTest extends V2TestBase {

  /**
   * Streaming projection of {@code _metadata.row_id} on a row-tracking-enabled table must emit
   * stable, monotonic row IDs matching batch reads.
   */
  @Test
  public void testStreaming_projectMetadataRowId(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableRowTracking' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Charlie')", tablePath));

    Dataset<Row> streamingDF =
        spark
            .readStream()
            .table(str("dsv2.delta.`%s`", tablePath))
            .select(col("id"), col("_metadata.row_id").as("row_id"));

    List<Row> actualRows = processStreamingQuery(streamingDF, "test_streaming_row_id");

    // Compare against batch read of the same projection - the row_id values must agree.
    List<Row> expectedRows =
        spark
            .sql(
                str(
                    "SELECT id, _metadata.row_id AS row_id FROM dsv2.delta.`%s` ORDER BY id",
                    tablePath))
            .collectAsList();

    assertDataEquals(actualRows, expectedRows);
    // Row IDs should be unique.
    Set<Long> rowIds = new HashSet<>();
    for (Row row : actualRows) {
      assertTrue(rowIds.add(row.getLong(1)), "row_id should be unique: " + row.getLong(1));
    }
  }

  /**
   * Streaming projection of {@code _metadata.row_commit_version} must emit the commit version that
   * created each row.
   */
  @Test
  public void testStreaming_projectMetadataRowCommitVersion(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableRowTracking' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Charlie')", tablePath));

    Dataset<Row> streamingDF =
        spark
            .readStream()
            .table(str("dsv2.delta.`%s`", tablePath))
            .select(col("id"), col("_metadata.row_commit_version").as("rcv"));

    List<Row> actualRows = processStreamingQuery(streamingDF, "test_streaming_rcv");

    List<Row> expectedRows =
        spark
            .sql(
                str(
                    "SELECT id, _metadata.row_commit_version AS rcv "
                        + "FROM dsv2.delta.`%s` ORDER BY id",
                    tablePath))
            .collectAsList();

    assertDataEquals(actualRows, expectedRows);
  }

  /**
   * Streaming projection of {@code _metadata.row_id} on a table that combines row tracking with
   * deletion vectors. DV filtering runs before row-tracking generation, and surviving rows must
   * carry their physical row IDs unchanged.
   */
  @Test
  public void testStreaming_projectMetadataRowId_withDV(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta "
                + "TBLPROPERTIES ("
                + "'delta.enableRowTracking' = 'true', "
                + "'delta.enableDeletionVectors' = 'true')",
            tablePath));
    // Insert enough rows for the DELETE to pick the DV path instead of rewriting whole files.
    spark
        .range(1000)
        .selectExpr("id", "cast(id as string) as name")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);
    // Low-selectivity, non-partition predicate so Delta uses DVs.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));

    DeltaLog deltaLog = DeltaLog.forTable(spark, tablePath);
    long numDVs =
        (long)
            deltaLog
                .update(false, Option.empty(), Option.empty())
                .numDeletionVectorsOpt()
                .getOrElse(() -> 0L);
    assertTrue(numDVs > 0, "Expected deletion vectors to be created, but none were found");

    Dataset<Row> streamingDF =
        spark
            .readStream()
            .table(str("dsv2.delta.`%s`", tablePath))
            .select(col("id"), col("_metadata.row_id").as("row_id"));

    List<Row> actualRows = processStreamingQuery(streamingDF, "test_streaming_row_id_dv");

    List<Row> expectedRows =
        spark
            .sql(
                str(
                    "SELECT id, _metadata.row_id AS row_id FROM dsv2.delta.`%s` ORDER BY id",
                    tablePath))
            .collectAsList();

    assertDataEquals(actualRows, expectedRows);
    assertEquals(500, actualRows.size(), "Expected only odd IDs after DV filtering");
    // Surviving rows must carry their physical row IDs (== id) under DV filtering.
    for (Row row : actualRows) {
      assertEquals(
          row.getLong(0),
          row.getLong(1),
          "row_id should preserve physical row identity under DV filtering");
    }
  }

  /**
   * Streaming projection of {@code _metadata.row_id} on a partitioned table whose partition column
   * sits in the middle of the DDL schema. {@code pruneColumns} must strip the partition column from
   * {@code requiredDataSchema} (it is not in the data files) while keeping {@code _metadata}. DDL
   * ordering must be preserved in the output.
   */
  @Test
  public void testStreaming_projectMetadataRowId_partitionInMiddle(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    // DDL order: id, part (partition), name - partition in position 1.
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, part STRING, name STRING) USING delta "
                + "PARTITIONED BY (part) "
                + "TBLPROPERTIES ('delta.enableRowTracking' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a', 'Alice'), (2, 'b', 'Bob')", tablePath));

    Dataset<Row> streamingDF =
        spark
            .readStream()
            .table(str("dsv2.delta.`%s`", tablePath))
            .select(col("id"), col("part"), col("name"), col("_metadata.row_id").as("row_id"));

    // Output columns: id, part, name, row_id - DDL order preserved, partition col included.
    assertArrayEquals(
        new String[] {"id", "part", "name", "row_id"}, streamingDF.schema().fieldNames());

    List<Row> actualRows = processStreamingQuery(streamingDF, "test_streaming_part_middle_row_id");

    // Verify batch matches streaming on data columns.
    List<Row> batchRows =
        spark
            .sql(
                str(
                    "SELECT id, part, name, _metadata.row_id AS row_id "
                        + "FROM dsv2.delta.`%s` ORDER BY id",
                    tablePath))
            .collectAsList();
    assertDataEquals(actualRows, batchRows);

    Set<Long> rowIds = actualRows.stream().map(r -> r.getLong(3)).collect(Collectors.toSet());
    assertEquals(2, rowIds.size(), "row_ids must be unique");
  }

  /**
   * Streaming projection of {@code _metadata.row_id} on a column-mapped table.
   *
   * <p>TODO: pre-existing bug (batch and streaming). {@link
   * io.delta.spark.internal.v2.utils.PartitionUtils#createDeltaParquetReaderFactory} expands {@code
   * _metadata} into hidden physical row-tracking columns (e.g. {@code _row-id-col-<uuid>}). Those
   * hidden names have no entry in the column-mapping metadata, so {@code
   * DeltaColumnMappingBase.createPhysicalSchema} throws {@code DELTA_CANNOT_RESOLVE_COLUMN}.
   */
  @Test
  public void testStreaming_projectMetadataRowId_columnMapping(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, user_name STRING) USING delta "
                + "TBLPROPERTIES ("
                + "'delta.columnMapping.mode' = 'name', "
                + "'delta.enableRowTracking' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));

    Dataset<Row> streamingDF =
        spark
            .readStream()
            .table(str("dsv2.delta.`%s`", tablePath))
            .select(col("id"), col("user_name"), col("_metadata.row_id").as("row_id"));

    List<Row> actualRows = processStreamingQuery(streamingDF, "test_streaming_cm_row_id");

    List<Row> batchRows =
        spark
            .sql(
                str(
                    "SELECT id, user_name, _metadata.row_id AS row_id "
                        + "FROM dsv2.delta.`%s` ORDER BY id",
                    tablePath))
            .collectAsList();
    assertDataEquals(actualRows, batchRows);

    Set<Long> rowIds = actualRows.stream().map(r -> r.getLong(2)).collect(Collectors.toSet());
    assertEquals(2, rowIds.size(), "row_ids must be unique");
  }

  /**
   * Per the Delta protocol ("Reader Requirements for Row Tracking"), readers MUST NOT expose {@code
   * row_id} or {@code row_commit_version} while reading CDC change data files. {@link
   * io.delta.spark.internal.v2.read.SparkScanBuilder} enforces this at scan-build time: {@code
   * defaultRequiredDataSchema} never includes {@code _metadata} in CDC mode, and {@code
   * pruneColumns} throws when CDC mode is active and {@code _metadata} is requested.
   *
   * <p>Mirror of {@code DeltaCDCStreamSuite#"CDC stream rejects reading row tracking metadata
   * fields"} for the DSv2 {@link io.delta.spark.internal.v2.catalog.SparkTable} code path.
   */
  @Test
  public void testStreaming_rejectRowTrackingInCDFMode(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta "
                + "TBLPROPERTIES ("
                + "'delta.enableChangeDataFeed' = 'true', "
                + "'delta.enableRowTracking' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));

    Exception ex =
        assertThrows(
            Exception.class,
            () -> {
              Dataset<Row> streamingDF =
                  spark
                      .readStream()
                      .option("readChangeFeed", "true")
                      .option("startingVersion", "0")
                      .table(str("dsv2.delta.`%s`", tablePath))
                      .select(col("id"), col("_metadata.row_id").as("row_id"));
              processStreamingQuery(streamingDF, "test_streaming_cdf_reject_row_id");
            });
    assertTrue(
        ex.getMessage().toLowerCase(java.util.Locale.ROOT).contains("row_id")
            || ex.getMessage().toLowerCase(java.util.Locale.ROOT).contains("row tracking")
            || ex.getMessage().toLowerCase(java.util.Locale.ROOT).contains("cannot be resolved"),
        "Expected error mentioning row_id or row tracking under CDC, got: " + ex.getMessage());
  }

  /**
   * Exercises all three dimensions simultaneously: partition column in the middle of the DDL
   * schema, column mapping enabled, and {@code _metadata.row_id} projection.
   *
   * <p>TODO: blocked by the same pre-existing column mapping + row tracking bug as {@link
   * #testStreaming_projectMetadataRowId_columnMapping}. Re-enable once that is fixed.
   */
  @Test
  public void testStreaming_projectMetadataRowId_partitionedWithColumnMapping(
      @TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    // DDL order: id, region (partition), score - partition sits in position 1.
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, region STRING, score DOUBLE) USING delta "
                + "PARTITIONED BY (region) "
                + "TBLPROPERTIES ("
                + "'delta.columnMapping.mode' = 'name', "
                + "'delta.enableRowTracking' = 'true')",
            tablePath));
    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create(1L, "eu", 9.5),
                RowFactory.create(2L, "us", 8.0),
                RowFactory.create(3L, "eu", 7.5)),
            new org.apache.spark.sql.types.StructType()
                .add("id", org.apache.spark.sql.types.DataTypes.LongType)
                .add("region", org.apache.spark.sql.types.DataTypes.StringType)
                .add("score", org.apache.spark.sql.types.DataTypes.DoubleType))
        .write()
        .format("delta")
        .mode("append")
        .partitionBy("region")
        .save(tablePath);

    Dataset<Row> streamingDF =
        spark
            .readStream()
            .table(str("dsv2.delta.`%s`", tablePath))
            .select(col("id"), col("region"), col("score"), col("_metadata.row_id").as("row_id"));

    // Output must respect DDL order: id, region, score, then the synthetic row_id.
    assertArrayEquals(
        new String[] {"id", "region", "score", "row_id"}, streamingDF.schema().fieldNames());

    List<Row> actualRows = processStreamingQuery(streamingDF, "test_streaming_part_cm_row_id");

    List<Row> batchRows =
        spark
            .sql(
                str(
                    "SELECT id, region, score, _metadata.row_id AS row_id "
                        + "FROM dsv2.delta.`%s` ORDER BY id",
                    tablePath))
            .collectAsList();
    assertDataEquals(actualRows, batchRows);

    Set<Long> rowIds = actualRows.stream().map(r -> r.getLong(3)).collect(Collectors.toSet());
    assertEquals(3, rowIds.size(), "row_ids must be unique across partitions");
  }

  /**
   * CDC streaming on a partitioned table. {@code pruneColumns} must strip both CDC tail columns
   * (injected later by {@code CDCReadFunction}) and the partition column (not in data files) while
   * keeping data columns intact. Partition column sits in the middle of the DDL schema.
   */
  @Test
  public void testStreaming_cdf_partitionedTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, part STRING, name STRING) USING delta "
                + "PARTITIONED BY (part) "
                + "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a', 'Alice'), (2, 'b', 'Bob')", tablePath));

    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("readChangeFeed", "true")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath))
            .select(col("id"), col("part"), col("name"));

    List<Row> actualRows = processStreamingQuery(streamingDF, "test_cdf_partitioned");

    List<Row> expectedRows =
        Arrays.asList(RowFactory.create(1L, "a", "Alice"), RowFactory.create(2L, "b", "Bob"));
    assertDataEquals(actualRows, expectedRows);
  }

  /**
   * CDC streaming on a column-mapped table. Physical Parquet column names differ from logical
   * names; {@code pruneColumns} must map logical names through column mapping correctly while also
   * stripping CDC tail columns.
   */
  @Test
  public void testStreaming_cdf_columnMapping(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, user_name STRING) USING delta "
                + "TBLPROPERTIES ("
                + "'delta.columnMapping.mode' = 'name', "
                + "'delta.enableChangeDataFeed' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));

    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("readChangeFeed", "true")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath))
            .select(col("id"), col("user_name"));

    List<Row> actualRows = processStreamingQuery(streamingDF, "test_cdf_cm");

    List<Row> expectedRows =
        Arrays.asList(RowFactory.create(1L, "Alice"), RowFactory.create(2L, "Bob"));
    assertDataEquals(actualRows, expectedRows);
  }

  /**
   * CDC streaming on a row-tracking-enabled table where {@code _metadata.row_id} is NOT selected.
   * The protocol prohibition only fires when row-tracking metadata is actually requested; a CDC
   * stream that reads only data columns must work even if the table has row tracking enabled.
   */
  @Test
  public void testStreaming_cdf_rowTrackingTable_noMetadataAccess(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta "
                + "TBLPROPERTIES ("
                + "'delta.enableChangeDataFeed' = 'true', "
                + "'delta.enableRowTracking' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));

    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("readChangeFeed", "true")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath))
            .select(col("id"), col("name"));

    List<Row> actualRows = processStreamingQuery(streamingDF, "test_cdf_rt_no_metadata");

    List<Row> expectedRows =
        Arrays.asList(RowFactory.create(1L, "Alice"), RowFactory.create(2L, "Bob"));
    assertDataEquals(actualRows, expectedRows);
  }

  /**
   * CDC streaming on a table that combines row tracking and column mapping, while attempting to
   * project {@code _metadata.row_id}. The rejection must fire before column-mapping translation:
   * {@code pruneColumns} detects CDC + {@code _metadata} and throws, regardless of column mapping.
   */
  @Test
  public void testStreaming_cdf_rowTrackingColumnMapping_rejectsMetadataAccess(
      @TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, user_name STRING) USING delta "
                + "TBLPROPERTIES ("
                + "'delta.columnMapping.mode' = 'name', "
                + "'delta.enableChangeDataFeed' = 'true', "
                + "'delta.enableRowTracking' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));

    Exception ex =
        assertThrows(
            Exception.class,
            () -> {
              Dataset<Row> streamingDF =
                  spark
                      .readStream()
                      .option("readChangeFeed", "true")
                      .option("startingVersion", "0")
                      .table(str("dsv2.delta.`%s`", tablePath))
                      .select(col("id"), col("_metadata.row_id").as("row_id"));
              processStreamingQuery(streamingDF, "test_cdf_rt_cm_reject");
            });
    assertTrue(
        ex.getMessage().toLowerCase(java.util.Locale.ROOT).contains("row_id")
            || ex.getMessage().toLowerCase(java.util.Locale.ROOT).contains("row tracking")
            || ex.getMessage().toLowerCase(java.util.Locale.ROOT).contains("cannot be resolved"),
        "Expected error mentioning row_id or row tracking, got: " + ex.getMessage());
  }
}
