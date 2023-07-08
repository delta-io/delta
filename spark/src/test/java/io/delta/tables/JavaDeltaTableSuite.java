/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.tables;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.test.*;
import org.apache.spark.sql.*;

import org.apache.spark.util.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.spark.sql.delta.DeltaSQLCommandJavaTest;

public class JavaDeltaTableSuite implements DeltaSQLCommandJavaTest {

  private transient SparkSession spark;
  private transient String input;


  @Before
  public void setUp() {
    // Trigger static initializer of TestData
    spark = buildSparkSession();
  }

  @After
  public void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  public void testAPI() {
    try {
      String input = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "input").toString();
      List<String> data = Arrays.asList("hello", "world");
      Dataset<Row> dataDF = spark.createDataset(data, Encoders.STRING()).toDF();
      List<Row> dataRows = dataDF.collectAsList();
      dataDF.write().format("delta").mode("overwrite").save(input);

      // Test creating DeltaTable by path
      DeltaTable table1 = DeltaTable.forPath(spark, input);
      QueryTest$.MODULE$.checkAnswer(table1.toDF(), dataRows);

      // Test creating DeltaTable by path picks up active SparkSession
      DeltaTable table2 = DeltaTable.forPath(input);
      QueryTest$.MODULE$.checkAnswer(table2.toDF(), dataRows);

      dataDF.write().format("delta").mode("overwrite").saveAsTable("deltaTable");

      // Test creating DeltaTable by name
      DeltaTable table3 = DeltaTable.forName(spark, "deltaTable");
      QueryTest$.MODULE$.checkAnswer(table3.toDF(), dataRows);

      // Test creating DeltaTable by name
      DeltaTable table4 = DeltaTable.forName("deltaTable");
      QueryTest$.MODULE$.checkAnswer(table4.toDF(), dataRows);

      // Test DeltaTable.as() creates subquery alias
      QueryTest$.MODULE$.checkAnswer(table2.as("tbl").toDF().select("tbl.value"), dataRows);

      // Test DeltaTable.isDeltaTable() is true for a Delta file path.
      Assert.assertTrue(DeltaTable.isDeltaTable(input));
    } finally {
      spark.sql("DROP TABLE IF EXISTS deltaTable");
    }
  }
}
