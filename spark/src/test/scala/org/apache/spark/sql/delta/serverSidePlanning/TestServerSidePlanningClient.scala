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

package org.apache.spark.sql.delta.serverSidePlanning

import java.util.Locale

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name

/**
 * Implementation of ServerSidePlanningClient that uses Spark SQL with input_file_name()
 * to discover the list of files in a table. This allows end-to-end testing without
 * a real server that can do server-side planning.
 *
 * This implementation works with any Spark-readable table format (Delta, Parquet, Iceberg, etc.)
 */
class TestServerSidePlanningClient(spark: SparkSession) extends ServerSidePlanningClient {

  override def planScan(database: String, table: String): ScanPlan = {
    val fullTableName = s"$database.$table"

    // Use input_file_name() to get the list of files
    // Query: SELECT DISTINCT input_file_name() FROM table
    val filesDF = spark.table(fullTableName)
      .select(input_file_name().as("file_path"))
      .distinct()

    // Collect file paths
    val filePaths = filesDF.collect().map(_.getString(0))

    // Get file metadata (size, format) from filesystem
    // scalastyle:off deltahadoopconfiguration
    val hadoopConf = spark.sessionState.newHadoopConf()
    // scalastyle:on deltahadoopconfiguration
    val files = filePaths.map { filePath =>
      // input_file_name() returns URL-encoded paths, decode them
      val decodedPath = java.net.URLDecoder.decode(filePath, "UTF-8")
      val path = new Path(decodedPath)
      val fs = path.getFileSystem(hadoopConf)
      val fileStatus = fs.getFileStatus(path)

      ScanFile(
        filePath = decodedPath,
        fileSizeInBytes = fileStatus.getLen,
        fileFormat = getFileFormat(path)
      )
    }.toSeq

    ScanPlan(files = files)
  }

  private def getFileFormat(path: Path): String = {
    // Scalastyle suppression needed: the caselocale regex incorrectly flags even correct usage
    // of toLowerCase(Locale.ROOT). Similar to PartitionUtils.scala and SchemaUtils.scala.
    // scalastyle:off caselocale
    val name = path.getName.toLowerCase(Locale.ROOT)
    // scalastyle:on caselocale
    if (name.endsWith(".parquet")) "parquet"
    else if (name.endsWith(".orc")) "orc"
    else if (name.endsWith(".avro")) "avro"
    else "unknown"
  }
}

/**
 * Factory for creating TestServerSidePlanningClient instances.
 */
class TestServerSidePlanningClientFactory extends ServerSidePlanningClientFactory {
  override def buildForCatalog(
      spark: SparkSession,
      catalogName: String): ServerSidePlanningClient = {
    new TestServerSidePlanningClient(spark)
  }
}
