/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal$;
import org.apache.spark.sql.delta.DeltaIllegalStateException;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.stats.StatisticsCollection;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import scala.Option;
import scala.collection.JavaConverters;

/** Tests for V2 streaming read operations. */
public class V2StreamingReadTest extends V2TestBase {

  @Test
  public void testStreamingReadWithStartingVersion(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();

    // Write version 0
    spark
        .createDataFrame(Arrays.asList(RowFactory.create(1, "Alice", 100.0)), TEST_SCHEMA)
        .write()
        .format("delta")
        .save(tablePath);

    // Write version 1
    spark
        .createDataFrame(Arrays.asList(RowFactory.create(2, "Bob", 200.0)), TEST_SCHEMA)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    // Write version 2
    spark
        .createDataFrame(Arrays.asList(RowFactory.create(3, "Charlie", 300.0)), TEST_SCHEMA)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    // Start streaming from version 0 - should read all three versions
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("startingVersion", "1").table(dsv2TableRef);
    assertTrue(streamingDF.isStreaming(), "Dataset should be streaming");

    // Process all batches - should have all data from versions 0, 1, and 2
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_with_starting_version");
    List<Row> expectedRows =
        Arrays.asList(RowFactory.create(2, "Bob", 200.0), RowFactory.create(3, "Charlie", 300.0));

    assertDataEquals(actualRows, expectedRows);
  }

  @Test
  public void testStreamingRead(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();

    // Write version 0
    spark
        .createDataFrame(Arrays.asList(RowFactory.create(1, "Alice", 100.0)), TEST_SCHEMA)
        .write()
        .format("delta")
        .save(tablePath);

    // Write version 1
    spark
        .createDataFrame(Arrays.asList(RowFactory.create(2, "Bob", 200.0)), TEST_SCHEMA)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    assertTrue(streamingDF.isStreaming(), "Dataset should be streaming");

    List<Row> actualRows = processStreamingQuery(streamingDF, "test_streaming_read");
    List<Row> expectedRows =
        Arrays.asList(RowFactory.create(1, "Alice", 100.0), RowFactory.create(2, "Bob", 200.0));

    assertDataEquals(actualRows, expectedRows);
  }

  /**
   * Tests that streaming read after stats recompute does not produce duplicate rows.
   *
   * <p>StatisticsCollection.recompute re-adds files with updated stats (dataChange=false), creating
   * duplicate AddFile entries in the log. The initial snapshot scan must use the selection vector
   * to filter out stale entries.
   */
  @Test
  public void testStreamingReadAfterStatsRecompute(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();

    // Write data with stats collection disabled - files will have no stats
    withSQLConf(
        "spark.databricks.delta.stats.collect",
        "false",
        () ->
            spark
                .range(10)
                .selectExpr("id", "cast(id as string) as value")
                .write()
                .format("delta")
                .save(tablePath));

    // Recompute statistics - this re-adds files with updated stats (dataChange=false),
    // creating duplicate AddFile entries in the log that must be filtered by selection vector
    DeltaLog deltaLog = DeltaLog.forTable(spark, tablePath);
    StatisticsCollection.recompute(
        spark,
        deltaLog,
        Option.empty(),
        JavaConverters.<Expression>asScalaBuffer(
                new ArrayList<>(List.of((Expression) Literal$.MODULE$.apply(true))))
            .toList(),
        af -> (Object) Boolean.TRUE);

    // Stream via V2 - should see each row exactly once, not duplicated
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    List<Row> actualRows = processStreamingQuery(streamingDF, "test_stats_recompute");

    List<Row> expectedRows =
        LongStream.range(0, 10)
            .mapToObj(i -> RowFactory.create(i, String.valueOf(i)))
            .collect(Collectors.toList());

    assertDataEquals(actualRows, expectedRows);
  }

  /**
   * Tests that streaming read correctly handles multiple deletion vectors on the same file.
   *
   * <p>When multiple DELETE operations are applied to the same file, each creates/updates a
   * deletion vector. The streaming initial snapshot should correctly apply the cumulative DV and
   * only return non-deleted rows.
   */
  @Test
  public void testStreamingReadWithMultipleDeletionVectors(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();

    // Create table with deletion vectors enabled
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (value INT) USING delta "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));

    // Write 10 rows (0-9) in a single file
    spark
        .range(10)
        .selectExpr("cast(id as int) as value")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    // Delete rows 0, 1, 2 - each creates/updates a DV on the same file
    spark.sql(str("DELETE FROM delta.`%s` WHERE value = 0", tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE value = 1", tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE value = 2", tablePath));

    // Verify DVs were created
    DeltaLog deltaLog = DeltaLog.forTable(spark, tablePath);
    long numDVs =
        (long)
            deltaLog
                .update(false, Option.empty(), Option.empty())
                .numDeletionVectorsOpt()
                .getOrElse(() -> 0L);
    assertTrue(numDVs > 0, "Expected deletion vectors to be created");

    // Stream via V2 - should see rows 3-9 only (rows 0, 1, 2 deleted)
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    List<Row> actualRows = processStreamingQuery(streamingDF, "test_multiple_dvs");

    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(3),
            RowFactory.create(4),
            RowFactory.create(5),
            RowFactory.create(6),
            RowFactory.create(7),
            RowFactory.create(8),
            RowFactory.create(9));

    assertDataEquals(actualRows, expectedRows);
  }

  /**
   * Verifies that stopping a V2 streaming query does not surface an exception.
   *
   * <p>When Spark stops a streaming query it calls {@link Thread#interrupt()} on the micro-batch
   * thread. If that thread is blocked inside Kernel's {@code DefaultJsonHandler} reading delta log
   * JSON files via NIO channels, the interrupt causes a {@link
   * java.nio.channels.ClosedByInterruptException} wrapped in a {@code KernelEngineException}. The
   * fix in {@code SparkMicroBatchStream.latestOffset()} and {@code
   * SparkMicroBatchStream.planInputPartitions()} re-wraps this as an {@link
   * java.io.UncheckedIOException} so Spark's {@code isInterruptedByStop} recognizes it as a clean
   * shutdown.
   */
  @Test
  public void testStreamingQueryStopDoesNotSurfaceException(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();

    // Write data
    spark
        .createDataFrame(Arrays.asList(RowFactory.create(1, "Alice", 100.0)), TEST_SCHEMA)
        .write()
        .format("delta")
        .save(tablePath);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    StreamingQuery query =
        streamingDF
            .writeStream()
            .format("memory")
            .queryName("test_stop_no_exception")
            .outputMode("append")
            .start();

    // Continuously write new commits so latestOffset() keeps reading fresh delta log JSON files
    // via NIO. This ensures Thread.interrupt() from stop() is likely to arrive while a channel
    // is open inside DefaultJsonHandler.hasNext(), directly exercising the fix.
    ExecutorService writer = Executors.newSingleThreadExecutor();
    try {
      writer.submit(
          () -> {
            for (int i = 0; i < 100 && query.isActive(); i++) {
              try {
                spark
                    .createDataFrame(
                        Arrays.asList(RowFactory.create(i + 2, "User" + i, (double) i * 10)),
                        TEST_SCHEMA)
                    .write()
                    .format("delta")
                    .mode("append")
                    .save(tablePath);
                Thread.sleep(20);
              } catch (Exception ignored) {
                return;
              }
            }
          });

      // Let the query process a few batches with active NIO reads before stopping.
      Thread.sleep(300);
      query.stop();
    } finally {
      // Don't interrupt — let the in-flight save() finish so Spark isn't still creating files
      // in @TempDir after we return. The writer exits on its own once query.isActive() is false.
      writer.shutdown();
      writer.awaitTermination(30, TimeUnit.SECONDS);
    }

    // Release cached DeltaLog references so @TempDir cleanup can delete the directory.
    DeltaLog.clearCache();

    // The stop should be clean — no exception should have been captured
    assertTrue(
        query.exception().isEmpty(),
        () ->
            "Expected no exception after query.stop(), but got: "
                + query.exception().get().toString());
  }

  // TODO(#5319): The V1 source does not detect nested field additions on restart. Port this
  //  validation to V1 for parity.
  @Test
  public void testNestedColumnAdditionDetectedOnRestart(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    // Create table with struct column: data STRUCT<x: INT>
    spark.sql(
        str("CREATE TABLE delta.`%s` (id STRING, data STRUCT<x: INT>) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES ('0', named_struct('x', 1))", tablePath));

    // Start streaming and process initial data
    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    StreamingQuery query =
        streamingDF
            .writeStream()
            .format("noop")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    query.processAllAvailable();
    query.stop();

    // Evolve struct via ALTER TABLE (metadata-only, no file deletion):
    // add nested field y -> data STRUCT<x: INT, y: INT>
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMNS (data.y INT)", tablePath));

    // Restart with stale DataFrame — should fail with schema mismatch
    StreamingQueryException ex =
        assertThrows(
            StreamingQueryException.class,
            () -> {
              StreamingQuery q =
                  streamingDF
                      .writeStream()
                      .format("noop")
                      .option("checkpointLocation", checkpointDir.getAbsolutePath())
                      .start();
              try {
                q.processAllAvailable();
              } finally {
                q.stop();
              }
            });
    assertInstanceOf(DeltaIllegalStateException.class, ex.cause());
    assertTrue(
        ex.getMessage().contains("DELTA_STREAMING_SCHEMA_MISMATCH_ON_RESTART"),
        "Expected DELTA_STREAMING_SCHEMA_MISMATCH_ON_RESTART but got: " + ex.getMessage());
  }

  // TODO(#6232): v2 source cannot adopt type widening schema change without refreshing the
  //  dataframe due to the lack of support in spark stream engine. Throw an error at stream
  //  start time to instruct user.
  @Test
  public void testNestedTypeWideningDetectedOnRestart(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    // Create table with struct column: data STRUCT<x: INT>
    spark.sql(
        str("CREATE TABLE delta.`%s` (id STRING, data STRUCT<x: INT>) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES ('0', named_struct('x', 1))", tablePath));

    // Start streaming and process initial data
    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    StreamingQuery query =
        streamingDF
            .writeStream()
            .format("noop")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    query.processAllAvailable();
    query.stop();

    // Widen nested field type via ALTER TABLE (metadata-only, no file deletion):
    // data.x INT -> data.x BIGINT
    spark.sql(
        str(
            "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')",
            tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ALTER COLUMN data.x TYPE BIGINT", tablePath));

    // Restart with stale DataFrame — should fail with schema mismatch
    StreamingQueryException ex =
        assertThrows(
            StreamingQueryException.class,
            () -> {
              StreamingQuery q =
                  streamingDF
                      .writeStream()
                      .format("noop")
                      .option("checkpointLocation", checkpointDir.getAbsolutePath())
                      .start();
              try {
                q.processAllAvailable();
              } finally {
                q.stop();
              }
            });
    assertInstanceOf(DeltaIllegalStateException.class, ex.cause());
    assertTrue(
        ex.getMessage().contains("DELTA_STREAMING_SCHEMA_MISMATCH_ON_RESTART"),
        "Expected DELTA_STREAMING_SCHEMA_MISMATCH_ON_RESTART but got: " + ex.getMessage());
  }

  // TODO(#6232): v2 source cannot adopt type widening schema change without refreshing the
  //  dataframe due to the lack of support in spark stream engine. Throw an error at stream
  //  start time to instruct user.
  @Test
  public void testNestedNullabilityRelaxDetectedOnRestart(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    // Create table via SQL DDL to preserve the NOT NULL constraint on the nested field.
    // DataFrame writes go through ImplicitMetadataOperation which calls schema.asNullable,
    // forcing all fields (including nested ones) to nullable — losing the NOT NULL.
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id STRING, data STRUCT<x: INT NOT NULL>) USING delta",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES ('0', named_struct('x', 1))", tablePath));

    // Start streaming and process initial data
    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    StreamingQuery query =
        streamingDF
            .writeStream()
            .format("noop")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    query.processAllAvailable();
    query.stop();

    // Relax nullability via ALTER TABLE (metadata-only, no file deletion):
    // data.x NOT NULL -> data.x nullable
    spark.sql(str("ALTER TABLE delta.`%s` ALTER COLUMN data.x DROP NOT NULL", tablePath));

    // Restart with stale DataFrame — should fail with schema mismatch
    StreamingQueryException ex =
        assertThrows(
            StreamingQueryException.class,
            () -> {
              StreamingQuery q =
                  streamingDF
                      .writeStream()
                      .format("noop")
                      .option("checkpointLocation", checkpointDir.getAbsolutePath())
                      .start();
              try {
                q.processAllAvailable();
              } finally {
                q.stop();
              }
            });
    assertInstanceOf(DeltaIllegalStateException.class, ex.cause());
    assertTrue(
        ex.getMessage().contains("DELTA_STREAMING_SCHEMA_MISMATCH_ON_RESTART"),
        "Expected DELTA_STREAMING_SCHEMA_MISMATCH_ON_RESTART but got: " + ex.getMessage());
  }

  @Test
  public void testStreamingReadPartitionColumnInMiddle(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, part LONG, col3 INT) "
                + "USING delta PARTITIONED BY (part)",
            tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)", tablePath));

    Dataset<Row> streamingDF = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    // User-facing schema must stay in DDL order so V2 matches V1 streaming behavior.
    assertArrayEquals(new String[] {"id", "part", "col3"}, streamingDF.schema().fieldNames());
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_partition_middle_ok");
    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(1L, 10L, 100),
            RowFactory.create(2L, 20L, 200),
            RowFactory.create(3L, 30L, 300));
    assertDataEquals(actualRows, expectedRows);
  }

  @Test
  public void testStreamingReadPartitionColumnAtEnd(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, col3 INT, part LONG) "
                + "USING delta PARTITIONED BY (part)",
            tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, 100, 10), (2, 200, 20), (3, 300, 30)", tablePath));

    Dataset<Row> streamingDF = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_partition_end_ok");
    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(1L, 100, 10L),
            RowFactory.create(2L, 200, 20L),
            RowFactory.create(3L, 300, 30L));
    assertDataEquals(actualRows, expectedRows);
  }

  @Test
  public void testStreamingReadMultiplePartitionColumns(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    // DDL order: a, p1, b, p2, c. Partition cols at positions 1 and 3; declared in reverse order.
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (a LONG, p1 STRING, b INT, p2 STRING, c DOUBLE) "
                + "USING delta PARTITIONED BY (p2, p1)",
            tablePath));
    // Use DataFrame writer so partition columns are routed correctly through the V1 write path.
    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create(1L, "x", 10, "y", 1.5),
                RowFactory.create(2L, "x", 20, "z", 2.5),
                RowFactory.create(3L, "w", 30, "y", 3.5)),
            new org.apache.spark.sql.types.StructType()
                .add("a", org.apache.spark.sql.types.DataTypes.LongType)
                .add("p1", org.apache.spark.sql.types.DataTypes.StringType)
                .add("b", org.apache.spark.sql.types.DataTypes.IntegerType)
                .add("p2", org.apache.spark.sql.types.DataTypes.StringType)
                .add("c", org.apache.spark.sql.types.DataTypes.DoubleType))
        .write()
        .format("delta")
        .mode("append")
        .partitionBy("p2", "p1")
        .save(tablePath);

    Dataset<Row> streamingDF = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    assertArrayEquals(new String[] {"a", "p1", "b", "p2", "c"}, streamingDF.schema().fieldNames());
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_partition_multi_ok");
    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(1L, "x", 10, "y", 1.5),
            RowFactory.create(2L, "x", 20, "z", 2.5),
            RowFactory.create(3L, "w", 30, "y", 3.5));
    assertDataEquals(actualRows, expectedRows);
  }
}
