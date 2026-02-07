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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
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
}
