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

package org.apache.spark.sql.delta;

import java.io.File;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.Utils;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;

/**
 * Minimal test coverage for V2 (Kernel) CREATE TABLE in STRICT mode.
 */
public class KernelSparkCreateTableSuite implements DeltaSQLCommandJavaTest {

  private transient SparkSession spark;
  private transient String tempPath;

  @Before
  public void setUp() {
    spark = buildSparkSession();
    spark.conf().set("spark.databricks.delta.v2.enableMode", "STRICT");
    tempPath = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "spark").toString();
  }

  @After
  public void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  public void testCreateTableIfNotExistsPathBased() {
    String path = new File(tempPath, "v2_create_path").getAbsolutePath();

    spark.sql(
        "CREATE TABLE IF NOT EXISTS delta.`" + path + "` (" +
            "id BIGINT, value DOUBLE" +
            ") USING delta");

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(path));
    Snapshot snapshot = deltaLog.update();

    Assert.assertEquals(0L, snapshot.version());
    StructType schema = snapshot.schema();
    Assert.assertArrayEquals(new String[] {"id", "value"}, schema.fieldNames());
  }

  @Test
  public void testCatalogCreateFailsInStrictMode() {
    try {
      spark.sql("CREATE TABLE strict_catalog_create (id BIGINT) USING delta");
      Assert.fail("Expected V2 CREATE to reject non-path identifiers in STRICT mode");
    } catch (UnsupportedOperationException expected) {
      Assert.assertTrue(
          expected.getMessage().contains("path-based")
              || expected.getMessage().contains("delta.`/path`"));
    }
  }
}
