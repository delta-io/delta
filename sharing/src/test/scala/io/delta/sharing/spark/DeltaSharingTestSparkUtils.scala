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

package org.apache.spark.sql.delta.sharing

import java.io.File

import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{
  getZoneId,
  stringToDate,
  stringToTimestamp,
  toJavaDate,
  toJavaTimestamp
}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.types.UTF8String

trait DeltaSharingTestSparkUtils extends DeltaSQLTestUtils {

  /**
   * Creates 3 temporary directories for use within a function.
   *
   * @param f function to be run with created temp directories
   */
  protected def withTempDirs(f: (File, File, File) => Unit): Unit = {
    withTempDir { file1 =>
      withTempDir { file2 =>
        withTempDir { file3 =>
          f(file1, file2, file3)
        }
      }
    }
  }

  protected def sqlDate(date: String): java.sql.Date = {
    toJavaDate(stringToDate(UTF8String.fromString(date)).get)
  }

  protected def sqlTimestamp(timestamp: String): java.sql.Timestamp = {
    toJavaTimestamp(
      stringToTimestamp(
        UTF8String.fromString(timestamp),
        getZoneId(SQLConf.get.sessionLocalTimeZone)
      ).get
    )
  }

  protected def createTable(tableName: String): Unit = {
    sql(s"""CREATE TABLE $tableName (c1 INT, c2 STRING, c3 date, c4 timestamp)
           |USING DELTA PARTITIONED BY (c2)
           |""".stripMargin)
  }

  protected def createTableForStreaming(tableName: String, enableDV: Boolean = false): Unit = {
    val tablePropertiesStr = if (enableDV) {
      "TBLPROPERTIES (delta.enableDeletionVectors = true)"
    } else {
      ""
    }
    sql(s"""
           |CREATE TABLE $tableName (value STRING)
           |USING DELTA
           |$tablePropertiesStr
           |""".stripMargin)
  }

  protected def createSimpleTable(tableName: String, enableCdf: Boolean): Unit = {
    val tablePropertiesStr = if (enableCdf) {
      """TBLPROPERTIES (
        |delta.minReaderVersion=1,
        |delta.minWriterVersion=4,
        |delta.enableChangeDataFeed = true)""".stripMargin
    } else {
      ""
    }
    sql(s"""CREATE TABLE $tableName (c1 INT, c2 STRING)
           |USING DELTA PARTITIONED BY (c2)
           |$tablePropertiesStr
           |""".stripMargin)
  }

  protected def createCMIdTableWithCdf(tableName: String): Unit = {
    sql(s"""CREATE TABLE $tableName (c1 INT, c2 STRING) USING DELTA PARTITIONED BY (c2)
           |TBLPROPERTIES ('delta.columnMapping.mode' = 'id', delta.enableChangeDataFeed = true)
           |""".stripMargin)
  }

  protected def createDVTableWithCdf(tableName: String): Unit = {
    sql(s"""CREATE TABLE $tableName (c1 INT, partition INT) USING DELTA PARTITIONED BY (partition)
           |TBLPROPERTIES (delta.enableDeletionVectors = true, delta.enableChangeDataFeed = true)
           |""".stripMargin)
  }

  protected def prepareProfileFile(tempDir: File): File = {
    val profileFile = new File(tempDir, "foo.share")
    FileUtils.writeStringToFile(
      profileFile,
      s"""{
         |  "shareCredentialsVersion": 1,
         |  "endpoint": "https://localhost:12345/not-used-endpoint",
         |  "bearerToken": "mock"
         |}""".stripMargin,
      "utf-8"
    )
    profileFile
  }
}

object DeltaSharingTestSparkUtils {
  def getHadoopConf(sparkConf: SparkConf): Configuration = {
    new SparkHadoopUtil().newConfiguration(sparkConf)
  }
}
