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

package org.apache.spark.sql.delta.files

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaLog}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructType}

class TransactionalWriteSuite extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {

  test("writing out an empty dataframe produces no AddFiles") {
    withTempDir { dir =>
      spark.range(100).write.format("delta").save(dir.getCanonicalPath)

      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val schema = new StructType().add("id", StringType)
      val emptyDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      assert(log.startTransaction().writeFiles(emptyDf).isEmpty)
    }
  }

  test("write data files to the data subdir") {
    withSQLConf(DeltaSQLConf.WRITE_DATA_FILES_TO_SUBDIR.key -> "true") {
      def validateDataSubdir(tablePath: String): Unit = {
        val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, tablePath)
        snapshot.allFiles.collect().foreach { f =>
          assert(f.path.startsWith("data/"))
        }
      }

      withTempDir { dir =>
        spark.range(100).toDF("id").write.format("delta").save(dir.getCanonicalPath)
        validateDataSubdir(dir.getCanonicalPath)
      }

      withTempDir { dir =>
        spark.range(100).toDF("id").withColumn("id1", column("id")).write.format("delta")
          .partitionBy("id").save(dir.getCanonicalPath)
        validateDataSubdir(dir.getCanonicalPath)
      }
    }

    withSQLConf(DeltaSQLConf.WRITE_DATA_FILES_TO_SUBDIR.key -> "false") {
      withTempDir { dir =>
        spark.range(100).toDF("id").write.format("delta").save(dir.getCanonicalPath)
        val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, dir.getCanonicalPath)
        snapshot.allFiles.collect().foreach { f =>
          assert(!f.path.startsWith("data/"))
        }
      }
    }
  }

  test("partitioned table defaults to writing partition columns to parquet files") {
    withTempDir { dir =>
      spark.range(100).toDF("id")
        .withColumn("partCol", column("id") % 5)
        .write.format("delta")
        .partitionBy("partCol")
        .save(dir.getCanonicalPath)

      val deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val snapshot = deltaLog.update()
      val files = snapshot.allFiles.collect()
      assert(files.nonEmpty)

      val logicalToPhysicalNameMap =
        DeltaColumnMapping.getLogicalNameToPhysicalNameMap(snapshot.schema)
      val physicalPartCol = logicalToPhysicalNameMap(Seq("partCol")).head

      files.foreach { file =>
        val filePath = DeltaFileOperations.absolutePath(deltaLog.dataPath.toString, file.path)
        val fileReader = ParquetFileReader.open(
          HadoopInputFile.fromPath(new Path(filePath.toString), new Configuration()))
        val parquetSchema = try {
          fileReader.getFooter.getFileMetaData.getSchema
        } finally {
          fileReader.close()
        }
        val fieldNames = parquetSchema.getFields.asScala.map(_.getName).toSet
        assert(fieldNames.contains(physicalPartCol),
          s"Partition column 'partCol' (physical: '$physicalPartCol') should be present " +
            s"in parquet file ${file.path}, but found fields: $fieldNames")
      }
    }
  }
}
