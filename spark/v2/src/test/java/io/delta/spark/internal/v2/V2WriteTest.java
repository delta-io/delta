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

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end tests for DSv2 batch write. Data is written through the V2 connector ({@code
 * dsv2.delta.`path`}) and read back to verify the Delta log was committed correctly.
 */
public class V2WriteTest extends V2TestBase {

  @Test
  public void writeAndReadBackSimpleTable(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();

    // Create table via V1 (establishes the Delta log with schema)
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));

    // Write through the V2 connector
    spark.sql(
        str(
            "INSERT INTO dsv2.delta.`%s` VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
            tablePath));

    // Read back via V2 and verify
    check(
        str("SELECT id, name FROM dsv2.delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "Alice"), row(2, "Bob"), row(3, "Charlie")));
  }

  @Test
  public void writeAndReadBackViaV1(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();

    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));

    // Write through V2
    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (10, 'X'), (20, 'Y')", tablePath));

    // Read back via V1 to confirm the Delta log is valid for both connectors
    Dataset<Row> v1Result = spark.read().format("delta").load(tablePath).orderBy("id");
    List<Row> rows = v1Result.collectAsList();

    assertEquals(2, rows.size());
    assertEquals(RowFactory.create(10, "X"), rows.get(0));
    assertEquals(RowFactory.create(20, "Y"), rows.get(1));
  }

  @Test
  public void multipleAppendsAccumulate(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();

    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));

    // First append via V2
    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (1, 'first')", tablePath));

    // Second append via V2
    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (2, 'second')", tablePath));

    // Both appends should be visible
    check(
        str("SELECT id, name FROM dsv2.delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "first"), row(2, "second")));
  }

  @Test
  public void writeEmptyResultSet(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();

    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));

    // Write an empty result set through V2
    spark.sql(
        str(
            "INSERT INTO dsv2.delta.`%s` SELECT * FROM VALUES (1, 'x') AS t(id, name) WHERE false",
            tablePath));

    // Table should still be empty
    long count = spark.sql(str("SELECT * FROM dsv2.delta.`%s`", tablePath)).count();
    assertEquals(0, count);
  }

  @Test
  public void v2WriteFollowedByV1WriteProducesCorrectLog(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();

    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));

    // V2 write
    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (1, 'v2-row')", tablePath));

    // V1 write
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'v1-row')", tablePath));

    // Both should be visible via V2 read
    check(
        str("SELECT id, name FROM dsv2.delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "v2-row"), row(2, "v1-row")));
  }
}
