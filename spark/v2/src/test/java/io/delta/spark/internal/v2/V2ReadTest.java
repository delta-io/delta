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
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

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
}
