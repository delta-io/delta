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
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal$;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.stats.StatisticsCollection;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import scala.Function1;
import scala.Option;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;

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
    spark.conf().set("spark.databricks.delta.stats.collect", "false");
    try {
      spark
          .range(10)
          .selectExpr("id", "cast(id as string) as value")
          .write()
          .format("delta")
          .save(tablePath);
    } finally {
      spark.conf().set("spark.databricks.delta.stats.collect", "true");
    }

    // Recompute statistics - this re-adds files with updated stats (dataChange=false),
    // creating duplicate AddFile entries in the log that must be filtered by selection vector
    DeltaLog deltaLog = DeltaLog.forTable(spark, tablePath);
    scala.collection.immutable.Seq<Expression> predicates =
        JavaConverters.<Expression>asScalaBuffer(
                new ArrayList<>(List.of((Expression) Literal$.MODULE$.apply(true))))
            .toList();
    Function1<AddFile, Object> fileFilter =
        new AbstractFunction1<AddFile, Object>() {
          @Override
          public Object apply(AddFile af) {
            return (Object) Boolean.TRUE;
          }
        };
    StatisticsCollection.recompute(spark, deltaLog, Option.empty(), predicates, fileFilter);

    // Stream via V2 - should see each row exactly once, not duplicated
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    List<Row> actualRows = processStreamingQuery(streamingDF, "test_stats_recompute");

    assertEquals(
        10,
        actualRows.size(),
        "Stats recompute should not cause duplicate rows in streaming read. Got: " + actualRows);
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
}
