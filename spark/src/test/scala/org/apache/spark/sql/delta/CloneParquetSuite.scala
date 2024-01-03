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

package org.apache.spark.sql.delta

import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class CloneParquetByPathSuite extends CloneParquetSuiteBase
{

  protected def withParquetTable(
      df: DataFrame, partCols: Seq[String] = Seq.empty[String])(
      func: ParquetIdent => Unit): Unit = {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      if (partCols.nonEmpty) {
        df.write.format("parquet").mode("overwrite").partitionBy(partCols: _*).save(tempDir)
      } else {
        df.write.format("parquet").mode("overwrite").save(tempDir)
      }

      func(ParquetIdent(tempDir, isTable = false))
    }
  }

  // CLONE doesn't support partitioned parquet table using path since it requires customer to
  // provide the partition schema in the command like `CONVERT TO DELTA`, but such an option is not
  // available in CLONE yet.
  testClone("clone partitioned parquet to delta table") { mode =>
    val df = spark.range(100)
      .withColumn("key1", col("id") % 4)
      .withColumn("key2", col("id") % 7 cast "String")

    withParquetTable(df, Seq("key1", "key2")) { sourceIdent =>
      val tableName = "cloneTable"
      withTable(tableName) {
        val se = intercept[SparkException] {
          sql(s"CREATE TABLE $tableName $mode CLONE $sourceIdent")
        }
        assert(se.getMessage.contains("Expecting 0 partition column(s)"))
      }
    }
  }
}

class CloneParquetByNameSuite extends CloneParquetSuiteBase
{

  protected def withParquetTable(
    df: DataFrame, partCols: Seq[String] = Seq.empty[String])(
    func: ParquetIdent => Unit): Unit = {
    val tableName = "parquet_table"
    withTable(tableName) {
      if (partCols.nonEmpty) {
        df.write.format("parquet").partitionBy(partCols: _*).saveAsTable(tableName)
      } else {
        df.write.format("parquet").saveAsTable(tableName)
      }

      func(ParquetIdent(tableName, isTable = true))
    }
  }

  testClone("clone partitioned parquet to delta table") { mode =>
    val df = spark.range(100)
      .withColumn("key1", col("id") % 4)
      .withColumn("key2", col("id") % 7 cast "String")

    withParquetTable(df, Seq("key1", "key2")) { sourceIdent =>
      val tableName = "cloneTable"
      withTable(tableName) {
        sql(s"CREATE TABLE $tableName $mode CLONE $sourceIdent")

        checkAnswer(spark.table(tableName), df)
      }
    }
  }

}
