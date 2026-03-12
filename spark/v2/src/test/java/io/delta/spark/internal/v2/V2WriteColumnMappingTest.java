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

import java.io.File;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for write operations on column-mapping-enabled Delta tables using the V2 connector.
 *
 * <p>Tables are created via the V1 path ({@code delta.`path`}), then writes and DML are executed
 * via the V2 catalog ({@code dsv2.delta.`path`}).
 */
public class V2WriteColumnMappingTest extends V2TestBase {

  // ---------- Append tests ----------

  @Test
  public void testAppendToNameModeTable(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();

    // Create table with column mapping name mode via V1
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id BIGINT, data STRING) "
                + "USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            path));

    // Insert initial data via V1
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", path));

    // Append via V2 connector
    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (3, 'c'), (4, 'd')", path));

    // Read back and verify all data is present
    check(
        str("SELECT id, data FROM dsv2.delta.`%s` ORDER BY id", path),
        List.of(row(1L, "a"), row(2L, "b"), row(3L, "c"), row(4L, "d")));
  }

  @Test
  public void testAppendToIdModeTable(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();

    // Create table with column mapping id mode via V1
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id BIGINT, data STRING) "
                + "USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id')",
            path));

    // Insert initial data via V1
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", path));

    // Append via V2 connector
    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (3, 'c'), (4, 'd')", path));

    // Read back and verify
    check(
        str("SELECT id, data FROM dsv2.delta.`%s` ORDER BY id", path),
        List.of(row(1L, "a"), row(2L, "b"), row(3L, "c"), row(4L, "d")));
  }

  @Test
  public void testWriteAfterColumnRename(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();

    // Create table with column mapping name mode
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id BIGINT, old_name STRING) "
                + "USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            path));

    // Insert data with original column name
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'before')", path));

    // Rename column
    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN old_name TO new_name", path));

    // Write new data via V2 using the renamed column
    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (2, 'after')", path));

    // Verify both old and new data readable with new column name
    check(
        str("SELECT id, new_name FROM dsv2.delta.`%s` ORDER BY id", path),
        List.of(row(1L, "before"), row(2L, "after")));
  }

  @Test
  public void testWriteAfterColumnDrop(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();

    // Create table with column mapping name mode
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id BIGINT, data STRING, extra INT) "
                + "USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            path));

    // Insert data with all columns
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a', 10)", path));

    // Drop column
    spark.sql(str("ALTER TABLE delta.`%s` DROP COLUMN extra", path));

    // Write new data via V2 (without dropped column)
    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (2, 'b')", path));

    // Verify all data readable
    check(
        str("SELECT id, data FROM dsv2.delta.`%s` ORDER BY id", path),
        List.of(row(1L, "a"), row(2L, "b")));
  }

  // ---------- COW DML on column mapping tables ----------

  @Test
  public void testDeleteOnColumnMappingTable(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id BIGINT, data STRING) "
                + "USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            path));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
            path));

    // DELETE via V2
    spark.sql(str("DELETE FROM dsv2.delta.`%s` WHERE id = 3", path));

    check(
        str("SELECT id, data FROM dsv2.delta.`%s` ORDER BY id", path),
        List.of(row(1L, "a"), row(2L, "b"), row(4L, "d"), row(5L, "e")));
  }

  @Test
  public void testUpdateOnColumnMappingTable(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id BIGINT, data STRING) "
                + "USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            path));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c')", path));

    // UPDATE via V2
    spark.sql(str("UPDATE dsv2.delta.`%s` SET data = 'updated' WHERE id = 2", path));

    check(
        str("SELECT id, data FROM dsv2.delta.`%s` ORDER BY id", path),
        List.of(row(1L, "a"), row(2L, "updated"), row(3L, "c")));
  }

  @Test
  public void testMergeOnColumnMappingTable(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id BIGINT, data STRING) "
                + "USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            path));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c')", path));

    // MERGE via V2: update existing + insert new
    spark.sql(
        str(
            "MERGE INTO dsv2.delta.`%s` t "
                + "USING (SELECT * FROM VALUES (2, 'updated'), (4, 'new') AS s(id, data)) s "
                + "ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET t.data = s.data "
                + "WHEN NOT MATCHED THEN INSERT (id, data) VALUES (s.id, s.data)",
            path));

    check(
        str("SELECT id, data FROM dsv2.delta.`%s` ORDER BY id", path),
        List.of(row(1L, "a"), row(2L, "updated"), row(3L, "c"), row(4L, "new")));
  }
}
