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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.delta.DeltaLog;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import scala.Option;

/** Tests for V2 batch read operations. */
public class V2ReadTest extends V2TestBase {

  @Test
  public void testBatchRead() {
    spark.sql(
        str("CREATE TABLE dsv2.%s.batch_read_test (id INT, name STRING, value DOUBLE)", nameSpace));

    check(str("SELECT * FROM dsv2.%s.batch_read_test", nameSpace), List.of());
  }

  @Test
  public void testColumnMappingRead(@TempDir File deltaTablePath) {
    String tablePath = deltaTablePath.getAbsolutePath();

    // Create a Delta table with column mapping enabled using name mode
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, user_name STRING, amount DOUBLE) "
                + "USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));

    // Insert test data
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)", tablePath));

    // Read through V2 and verify
    check(
        str("SELECT * FROM dsv2.delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "Alice", 100.0), row(2, "Bob", 200.0)));
  }

  @Test
  public void testDeletionVectorRead(@TempDir File tempDir) throws Exception {
    // Create a directory with space in the name to test URL encoding handling
    File dirWithSpace = new File(tempDir, "my table");
    Files.createDirectories(dirWithSpace.toPath());
    String tablePath = dirWithSpace.getAbsolutePath();

    // Create a Delta table with deletion vectors enabled.
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, value STRING) "
                + "USING delta "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));

    // Insert enough data so that DELETE creates DVs instead of rewriting the file.
    // Use spark.range() to generate more rows.
    spark
        .range(1000)
        .selectExpr("id", "cast(id as string) as value")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    // Delete some rows to create deletion vectors (not whole file deletions).
    spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));

    // Verify that deletion vectors were actually created.
    DeltaLog deltaLog = DeltaLog.forTable(spark, tablePath);
    long numDVs =
        (long)
            deltaLog
                .update(false, Option.empty(), Option.empty())
                .numDeletionVectorsOpt()
                .getOrElse(() -> 0L);
    assertTrue(numDVs > 0, "Expected deletion vectors to be created, but none were found");

    // Read through V2 and verify deleted rows are filtered out (only odd ids remain).
    long count = spark.sql(str("SELECT * FROM dsv2.delta.`%s`", tablePath)).count();
    // 500 odd numbers from 0-999: 1, 3, 5, ..., 999
    assertTrue(count == 500, "Expected 500 rows after DV filtering, got " + count);
  }

  /**
   * V2 batch read works correctly with a partition column declared in the middle of the schema.
   * Included alongside streaming tests to validate consistent behavior across batch and streaming
   * paths (V2 batch binds output from {@code Scan.readSchema()} via {@code V2ScanRelationPushDown},
   * so it's unaffected by the DDL/reader-order mismatch that the streaming path fixes).
   */
  @Test
  public void testBatchReadPartitionColumnInMiddle(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, part LONG, col3 INT) "
                + "USING delta PARTITIONED BY (part)",
            tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)", tablePath));

    // User-facing schema stays in DDL order.
    assertArrayEquals(
        new String[] {"id", "part", "col3"},
        spark.sql(str("SELECT * FROM dsv2.delta.`%s`", tablePath)).schema().fieldNames());
    check(
        str("SELECT * FROM dsv2.delta.`%s` ORDER BY id", tablePath),
        List.of(row(1L, 10L, 100), row(2L, 20L, 200), row(3L, 30L, 300)));
  }

  /**
   * Control test: V2 batch read works when the partition column is declared at the END of the
   * schema.
   */
  @Test
  public void testBatchReadPartitionColumnAtEnd(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, col3 INT, part LONG) "
                + "USING delta PARTITIONED BY (part)",
            tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, 100, 10), (2, 200, 20), (3, 300, 30)", tablePath));

    check(
        str("SELECT * FROM dsv2.delta.`%s` ORDER BY id", tablePath),
        List.of(row(1L, 100, 10L), row(2L, 200, 20L), row(3L, 300, 30L)));
  }

  /**
   * Multiple partition columns interleaved with data columns, declared in reverse order in {@code
   * PARTITIONED BY}.
   */
  @Test
  public void testBatchReadMultiplePartitionColumns(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (a LONG, p1 STRING, b INT, p2 STRING, c DOUBLE) "
                + "USING delta PARTITIONED BY (p2, p1)",
            tablePath));
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

    assertArrayEquals(
        new String[] {"a", "p1", "b", "p2", "c"},
        spark.sql(str("SELECT * FROM dsv2.delta.`%s`", tablePath)).schema().fieldNames());
    check(
        str("SELECT * FROM dsv2.delta.`%s` ORDER BY a", tablePath),
        List.of(
            row(1L, "x", 10, "y", 1.5), row(2L, "x", 20, "z", 2.5), row(3L, "w", 30, "y", 3.5)));
  }
}
