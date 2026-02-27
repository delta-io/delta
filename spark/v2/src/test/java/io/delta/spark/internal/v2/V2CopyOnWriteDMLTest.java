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
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for Copy-on-Write DML operations (DELETE, UPDATE, MERGE) using the V2 connector (SparkTable
 * with SupportsRowLevelOperations).
 *
 * <p>Tables are created and populated via the V1 path ({@code delta.`path`}), then DML is executed
 * via the V2 catalog ({@code dsv2.delta.`path`}).
 */
public class V2CopyOnWriteDMLTest extends V2TestBase {

  private void createAndPopulate(String tablePath) {
    spark.sql(str("CREATE TABLE delta.`%s` (id BIGINT, data STRING) USING delta", tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
            tablePath));
  }

  // ---------- DELETE ----------

  @Test
  public void testDeleteSingleRow(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    createAndPopulate(path);

    spark.sql(str("DELETE FROM dsv2.delta.`%s` WHERE id = 3", path));

    check(
        str("SELECT id, data FROM dsv2.delta.`%s` ORDER BY id", path),
        List.of(row(1L, "a"), row(2L, "b"), row(4L, "d"), row(5L, "e")));
  }

  @Test
  public void testDeleteMultipleRows(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    createAndPopulate(path);

    spark.sql(str("DELETE FROM dsv2.delta.`%s` WHERE id >= 4", path));

    check(
        str("SELECT id, data FROM dsv2.delta.`%s` ORDER BY id", path),
        List.of(row(1L, "a"), row(2L, "b"), row(3L, "c")));
  }

  @Test
  public void testDeleteAllRows(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    createAndPopulate(path);

    spark.sql(str("DELETE FROM dsv2.delta.`%s` WHERE true", path));

    check(str("SELECT id, data FROM dsv2.delta.`%s`", path), List.of());
  }

  @Test
  public void testDeleteNoRows(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    createAndPopulate(path);

    spark.sql(str("DELETE FROM dsv2.delta.`%s` WHERE id = 999", path));

    check(
        str("SELECT id, data FROM dsv2.delta.`%s` ORDER BY id", path),
        List.of(row(1L, "a"), row(2L, "b"), row(3L, "c"), row(4L, "d"), row(5L, "e")));
  }

  // ---------- UPDATE ----------

  @Test
  public void testUpdateSingleRow(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    createAndPopulate(path);

    spark.sql(str("UPDATE dsv2.delta.`%s` SET data = 'updated' WHERE id = 2", path));

    check(
        str("SELECT id, data FROM dsv2.delta.`%s` ORDER BY id", path),
        List.of(row(1L, "a"), row(2L, "updated"), row(3L, "c"), row(4L, "d"), row(5L, "e")));
  }

  @Test
  public void testUpdateMultipleRows(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    createAndPopulate(path);

    spark.sql(str("UPDATE dsv2.delta.`%s` SET data = 'big' WHERE id > 3", path));

    check(
        str("SELECT id, data FROM dsv2.delta.`%s` ORDER BY id", path),
        List.of(row(1L, "a"), row(2L, "b"), row(3L, "c"), row(4L, "big"), row(5L, "big")));
  }

  @Test
  public void testUpdateAllRows(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    createAndPopulate(path);

    spark.sql(str("UPDATE dsv2.delta.`%s` SET data = 'all'", path));

    check(
        str("SELECT id, data FROM dsv2.delta.`%s` ORDER BY id", path),
        List.of(row(1L, "all"), row(2L, "all"), row(3L, "all"), row(4L, "all"), row(5L, "all")));
  }

  // ---------- MERGE ----------

  @Test
  public void testMergeMatchedUpdate(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    createAndPopulate(path);

    spark.sql(
        str(
            "MERGE INTO dsv2.delta.`%s` t "
                + "USING (SELECT 2 AS id, 'merged' AS data) s "
                + "ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET t.data = s.data",
            path));

    check(
        str("SELECT id, data FROM dsv2.delta.`%s` ORDER BY id", path),
        List.of(row(1L, "a"), row(2L, "merged"), row(3L, "c"), row(4L, "d"), row(5L, "e")));
  }

  @Test
  public void testMergeMatchedDelete(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    createAndPopulate(path);

    spark.sql(
        str(
            "MERGE INTO dsv2.delta.`%s` t "
                + "USING (SELECT 3 AS id) s "
                + "ON t.id = s.id "
                + "WHEN MATCHED THEN DELETE",
            path));

    check(
        str("SELECT id, data FROM dsv2.delta.`%s` ORDER BY id", path),
        List.of(row(1L, "a"), row(2L, "b"), row(4L, "d"), row(5L, "e")));
  }

  @Test
  public void testMergeNotMatchedInsert(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    createAndPopulate(path);

    spark.sql(
        str(
            "MERGE INTO dsv2.delta.`%s` t "
                + "USING (SELECT 6 AS id, 'f' AS data) s "
                + "ON t.id = s.id "
                + "WHEN NOT MATCHED THEN INSERT (id, data) VALUES (s.id, s.data)",
            path));

    check(
        str("SELECT id, data FROM dsv2.delta.`%s` ORDER BY id", path),
        List.of(
            row(1L, "a"), row(2L, "b"), row(3L, "c"), row(4L, "d"), row(5L, "e"), row(6L, "f")));
  }

  @Test
  public void testMergeUpdateAndInsert(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    createAndPopulate(path);

    spark.sql(
        str(
            "MERGE INTO dsv2.delta.`%s` t "
                + "USING (SELECT * FROM VALUES (2, 'updated'), (6, 'new') AS s(id, data)) s "
                + "ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET t.data = s.data "
                + "WHEN NOT MATCHED THEN INSERT (id, data) VALUES (s.id, s.data)",
            path));

    check(
        str("SELECT id, data FROM dsv2.delta.`%s` ORDER BY id", path),
        List.of(
            row(1L, "a"),
            row(2L, "updated"),
            row(3L, "c"),
            row(4L, "d"),
            row(5L, "e"),
            row(6L, "new")));
  }

  // ---------- Sequential ----------

  @Test
  public void testSequentialDeleteThenUpdate(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    createAndPopulate(path);

    spark.sql(str("DELETE FROM dsv2.delta.`%s` WHERE id = 1", path));
    spark.sql(str("UPDATE dsv2.delta.`%s` SET data = 'x' WHERE id = 2", path));

    check(
        str("SELECT id, data FROM dsv2.delta.`%s` ORDER BY id", path),
        List.of(row(2L, "x"), row(3L, "c"), row(4L, "d"), row(5L, "e")));
  }
}
