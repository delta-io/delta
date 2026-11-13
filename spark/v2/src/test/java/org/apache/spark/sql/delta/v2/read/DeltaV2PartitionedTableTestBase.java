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
package org.apache.spark.sql.delta.v2.read;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.delta.v2.DeltaV2TestBase;
import org.apache.spark.sql.delta.v2.catalog.DeltaV2Table;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import scala.Option;

/**
 * Shared fixture for read-path tests that run against one partitioned Delta table (see {@link
 * #createPartitionedTable}) created once per test class.
 *
 * <p>A subclass calls {@link #setUpSharedPartitionedTable} from its own {@code @BeforeAll} method,
 * passing a class-unique table name and a {@code @TempDir} directory. Each test instance then gets
 * a {@link #table} over that table. Class-wide SQL confs go through {@link #setClassConf}, which
 * records the prior value so {@link #restoreClassConfs} can put it back after the class.
 */
abstract class DeltaV2PartitionedTableTestBase extends DeltaV2TestBase {

  static String tablePath;
  static String tableName;

  /** Values the class-wide confs had before {@link #setClassConf} first overrode them. */
  private static final Map<String, Option<String>> originalConfs = new LinkedHashMap<>();

  /** Creates the shared partitioned table {@code name} at {@code tempDir}. */
  static void setUpSharedPartitionedTable(String name, File tempDir) {
    tableName = name;
    tablePath = tempDir.getAbsolutePath();
    createPartitionedTable(tableName, tablePath);
  }

  /** Sets a SQL conf for the whole test class; {@link #restoreClassConfs} undoes it. */
  static void setClassConf(String key, String value) {
    originalConfs.putIfAbsent(key, spark.conf().getOption(key));
    spark.conf().set(key, value);
  }

  /**
   * Restores every conf set through {@link #setClassConf} (unsetting ones that were absent). JUnit
   * runs this before the inherited {@code DeltaV2TestBase.tearDownSpark}, so the session is still
   * live.
   */
  @AfterAll
  static void restoreClassConfs() {
    if (spark != null) {
      for (Map.Entry<String, Option<String>> entry : originalConfs.entrySet()) {
        if (entry.getValue().isDefined()) {
          spark.conf().set(entry.getKey(), entry.getValue().get());
        } else {
          spark.conf().unset(entry.getKey());
        }
      }
    }
    originalConfs.clear();
  }

  final CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(new HashMap<>());

  final DeltaV2Table table =
      new DeltaV2Table(
          Identifier.of(new String[] {"spark_catalog", "default"}, tableName), tablePath, options);
}
