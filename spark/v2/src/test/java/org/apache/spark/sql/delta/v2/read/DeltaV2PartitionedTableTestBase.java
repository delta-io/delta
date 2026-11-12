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
package org.apache.spark.sql.delta.v2.read;

import java.io.File;
import java.util.HashMap;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.delta.v2.DeltaV2TestBase;
import org.apache.spark.sql.delta.v2.catalog.DeltaV2Table;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Shared fixture for read-path tests that run against one partitioned Delta table (see {@link
 * #createPartitionedTable}) created once per test class.
 *
 * <p>A subclass calls {@link #setUpSharedPartitionedTable} from its own {@code @BeforeAll} method,
 * passing a class-unique table name and a {@code @TempDir} directory. Each test instance then gets
 * a {@link #table} over that table. SQL confs set here need no explicit restore: {@link
 * DeltaV2TestBase} creates a new Spark session for every test class and stops it in
 * {@code @AfterAll}.
 */
public abstract class DeltaV2PartitionedTableTestBase extends DeltaV2TestBase {

  protected static String tablePath;
  protected static String tableName;

  /** Creates the shared partitioned table {@code name} at {@code tempDir}. */
  protected static void setUpSharedPartitionedTable(String name, File tempDir) {
    tableName = name;
    tablePath = tempDir.getAbsolutePath();
    createPartitionedTable(tableName, tablePath);
  }

  protected final CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(new HashMap<>());

  protected final DeltaV2Table table =
      new DeltaV2Table(
          Identifier.of(new String[] {"spark_catalog", "default"}, tableName), tablePath, options);
}
