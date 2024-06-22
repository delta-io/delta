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

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.commands.CloneParquetSource
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession

trait CloneParquetSuiteBase extends QueryTest
  with DeltaSQLCommandTest
  with SharedSparkSession {

  // Identifier to represent a Parquet source
  protected case class ParquetIdent(name: String, isTable: Boolean) {

    override def toString: String = if (isTable) name else s"parquet.`$name`"

    def toTableIdent: TableIdentifier =
      if (isTable) TableIdentifier(name) else TableIdentifier(name, Some("parquet"))

    def toCloneSource: CloneParquetSource = {
      val catalogTableOpt =
        if (isTable) Some(spark.sessionState.catalog.getTableMetadata(toTableIdent)) else None
      CloneParquetSource(toTableIdent, catalogTableOpt, spark)
    }
  }

  protected def supportedModes: Seq[String] = Seq("SHALLOW")

  protected def testClone(testName: String)(f: String => Unit): Unit =
    supportedModes.foreach { mode => test(s"$testName - $mode") { f(mode) } }

  protected def withParquetTable(
      df: DataFrame, partCols: Seq[String] = Seq.empty[String])(func: ParquetIdent => Unit): Unit

  protected def validateBlob(
      blob: Map[String, Any],
      mode: String,
      source: CloneParquetSource,
      target: DeltaLog): Unit = {
    // scalastyle:off deltahadoopconfiguration
    val hadoopConf = spark.sessionState.newHadoopConf()
    // scalastyle:on deltahadoopconfiguration

    val sourcePath = source.dataPath
    val sourceFs = sourcePath.getFileSystem(hadoopConf)
    val qualifiedSourcePath = sourceFs.makeQualified(sourcePath)

    val targetPath = target.dataPath
    val targetFs = targetPath.getFileSystem(hadoopConf)
    val qualifiedTargetPath = targetFs.makeQualified(targetPath)

    assert(blob("sourcePath") === qualifiedSourcePath.toString)
    assert(blob("target") === qualifiedTargetPath.toString)
    assert(blob("sourceTableSize") === source.sizeInBytes)
    assert(blob("sourceNumOfFiles") === source.numOfFiles)
    assert(blob("partitionBy") === source.metadata.partitionColumns)
  }

  testClone("validate clone metrics") { mode =>
    val df = spark.range(100).withColumn("key", col("id") % 3)
    withParquetTable(df) { sourceIdent =>
      val tableName = "cloneTable"
      withTable(tableName) {
        val allLogs = Log4jUsageLogger.track {
          sql(s"CREATE TABLE $tableName $mode CLONE $sourceIdent")
        }

        val source = sourceIdent.toCloneSource
        val target = DeltaLog.forTable(spark, TableIdentifier(tableName))

        val blob = JsonUtils.fromJson[Map[String, Any]](allLogs
          .filter(_.metric == "tahoeEvent")
          .filter(_.tags.get("opType").contains("delta.clone"))
          .filter(_.blob.contains("source"))
          .map(_.blob).last)
        validateBlob(blob, mode, source, target)

        val sourceMetadata = source.metadata
        val targetMetadata = target.update().metadata

        assert(sourceMetadata.schema === targetMetadata.schema)
        assert(sourceMetadata.configuration === targetMetadata.configuration)
        assert(sourceMetadata.dataSchema === targetMetadata.dataSchema)
        assert(sourceMetadata.partitionColumns === targetMetadata.partitionColumns)
      }
    }
  }

  testClone("clone non-partitioned parquet to delta table") { mode =>
    val df = spark.range(100)
      .withColumn("key1", col("id") % 4)
      .withColumn("key2", col("id") % 7 cast "String")

    withParquetTable(df) { sourceIdent =>
      val tableName = "cloneTable"
      withTable(tableName) {
        sql(s"CREATE TABLE $tableName $mode CLONE $sourceIdent")

        checkAnswer(spark.table(tableName), df)
      }
    }
  }

  testClone("clone non-partitioned parquet to delta path") { mode =>
    val df = spark.range(100)
      .withColumn("key1", col("id") % 4)
      .withColumn("key2", col("id") % 7 cast "String")

    withParquetTable(df) { sourceIdent =>
      withTempDir { dir =>
        val deltaDir = dir.getCanonicalPath
        sql(s"CREATE TABLE delta.`$deltaDir` $mode CLONE $sourceIdent")

        checkAnswer(spark.read.format("delta").load(deltaDir), df)
      }
    }
  }
}
