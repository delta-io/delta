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
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.functions.input_file_name

/**
 * Implementation of ServerSidePlanningClient that uses Spark SQL with input_file_name()
 * to discover the list of files in a table. This allows end-to-end testing without
 * a real server that can do server-side planning.
 *
 * This implementation works with any Spark-readable table format (Delta, Parquet, Iceberg, etc.)
 *
 * @param spark SparkSession for table access
 * @param credentials Optional credentials to include in scan plans
 *                    (for testing credential injection)
 * @param pathRewriteScheme Optional scheme to rewrite file paths to (for testing with credential
 *                          filesystems). For example, pass "s3a" to rewrite file:// paths to s3a://
 */
class TestServerSidePlanningClient(
    spark: SparkSession,
    credentials: Option[StorageCredentials] = None,
    pathRewriteScheme: Option[String] = None) extends ServerSidePlanningClient {

  override def planScan(database: String, table: String): ScanPlan = {
    val fullTableName = s"$database.$table"

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

        // Optionally rewrite path scheme for testing with credential filesystems
        val finalPath = pathRewriteScheme match {
          case Some(newScheme) =>
            // Rewrite file:// paths to use the specified scheme
            // Example: file:///tmp/bucket/key -> s3a://tmp/bucket/key
            val uri = path.toUri
            if (uri.getScheme == "file") {
              s"$newScheme://${uri.getPath}"
            } else {
              decodedPath
            }
          case None => decodedPath
        }

        ScanFile(
          filePath = finalPath,
          fileSizeInBytes = fileStatus.getLen,
          fileFormat = getFileFormat(path)
        )
      }.toSeq

      ScanPlan(files = files, credentials = credentials)
    } finally {
      // Restore original config value
      originalConfigValue match {
        case Some(value) => spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, value)
        case None => spark.conf.unset(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
      }
    }
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
  override def buildFromMetadata(
      spark: SparkSession,
      metadata: ServerSidePlanningMetadata): ServerSidePlanningClient = {
    // TestServerSidePlanningClient doesn't need metadata - it reads files via Spark
    new TestServerSidePlanningClient(spark)
  }
}

/**
 * Factory for creating TestServerSidePlanningClient instances with credentials.
 * Used for testing credential injection.
 *
 * @param credentials Credentials to include in scan plans
 * @param pathRewriteScheme Optional scheme to rewrite paths to (e.g., "s3a" for S3 testing)
 */
class TestServerSidePlanningClientFactoryWithCredentials(
    credentials: Option[StorageCredentials],
    pathRewriteScheme: Option[String] = None) extends ServerSidePlanningClientFactory {

  override def buildFromMetadata(
      spark: SparkSession,
      metadata: ServerSidePlanningMetadata): ServerSidePlanningClient = {
    new TestServerSidePlanningClient(spark, credentials, pathRewriteScheme)
  }
}
