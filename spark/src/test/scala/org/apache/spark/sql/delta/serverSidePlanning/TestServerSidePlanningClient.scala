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

package org.apache.spark.sql.delta.serverSidePlanning

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.sources.Filter

/**
 * Implementation of ServerSidePlanningClient that uses Spark SQL with input_file_name()
 * to discover the list of files in a table. This allows end-to-end testing without
 * a real server that can do server-side planning.
 *
 * Also captures filter/projection parameters for test verification via companion object.
 */
class TestServerSidePlanningClient(spark: SparkSession) extends ServerSidePlanningClient {

  override def planScan(
      databaseName: String,
      table: String,
      filterOption: Option[Filter] = None,
      projectionOption: Option[Seq[String]] = None): ScanPlan = {
    // Capture filter and projection for test verification
    TestServerSidePlanningClient.capturedFilter = filterOption
    TestServerSidePlanningClient.capturedProjection = projectionOption

    val fullTableName = s"$databaseName.$table"

    // Temporarily disable server-side planning to avoid infinite recursion
    // when this test client internally loads the table
    val originalConfigValue = spark.conf.getOption(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
    spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "false")

    try {
      // Use input_file_name() to get the list of files
      // Query: SELECT DISTINCT input_file_name() FROM table
      val filesDF = spark.table(fullTableName)
        .select(input_file_name().as("file_path"))
        .distinct()

      // Collect file paths
      val filePaths = filesDF.collect().map(_.getString(0))

      // Get file metadata (size, format) from filesystem
      // scalastyle:off deltahadoopconfiguration
      // The rule prevents accessing Hadoop conf on executors where it could use wrong credentials
      // for multi-catalog scenarios. Safe here: test-only code simulating server filesystem access.
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
    } finally {
      // Restore original config value
      originalConfigValue match {
        case Some(value) => spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, value)
        case None => spark.conf.unset(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
      }
    }
  }

  private def getFileFormat(path: Path): String = "parquet"
}

/**
 * Companion object for TestServerSidePlanningClient.
 * Stores captured pushdown parameters (filter, projection) for test verification.
 */
object TestServerSidePlanningClient {
  private var capturedFilter: Option[Filter] = None
  private var capturedProjection: Option[Seq[String]] = None

  def getCapturedFilter: Option[Filter] = capturedFilter
  def getCapturedProjection: Option[Seq[String]] = capturedProjection
  def clearCaptured(): Unit = {
    capturedFilter = None
    capturedProjection = None
  }
}

/**
 * Factory for creating TestServerSidePlanningClient instances.
 */
class TestServerSidePlanningClientFactory extends ServerSidePlanningClientFactory {
  override def buildClient(
      spark: SparkSession,
      metadata: ServerSidePlanningMetadata): ServerSidePlanningClient = {
    new TestServerSidePlanningClient(spark)
  }
}
