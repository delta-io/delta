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

import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.Utils;

import static org.junit.Assert.assertTrue;

/**
 * Minimal POC test for V2 (Kernel) CREATE TABLE in STRICT mode.
 *
 * This test intentionally validates creation via metadata/log existence
 * because V2 does not yet implement data writes.
 */
public class KernelSparkCreateTableSuite implements DeltaSQLCommandJavaTest {

  private transient SparkSession spark;
  private transient String tempPath;

  @Before
  public void setUp() {
    spark = buildSparkSession();
    spark.conf().set("spark.databricks.delta.v2.enableMode", "STRICT");
    tempPath =
        Utils.createTempDir(System.getProperty("java.io.tmpdir"), "delta_v2_create").toString();
  }

  @After
  public void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  public void testCreateTableIfNotExistsPathBased() throws Exception {
    String tablePath = tempPath + "/tbl";

    spark.sql(
        String.format(
            "CREATE TABLE IF NOT EXISTS delta.`%s` (id BIGINT, value DOUBLE) USING delta",
            tablePath));

    Path logFile = new Path(tablePath, "_delta_log/00000000000000000000.json");
    FileSystem fs = logFile.getFileSystem(spark.sessionState().newHadoopConf());
    assertTrue("Expected v0 log file to exist", fs.exists(logFile));

    StructType schema = spark.read().format("delta").load(tablePath).schema();
    assertTrue(Arrays.equals(schema.fieldNames(), new String[] {"id", "value"}));
  }
}
