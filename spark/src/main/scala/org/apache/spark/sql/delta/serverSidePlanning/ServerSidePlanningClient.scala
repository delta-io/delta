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

import org.apache.spark.sql.SparkSession

/**
 * Simple data class representing a file to scan.
 * No dependencies on Iceberg types.
 */
case class ScanFile(
  filePath: String,
  fileSizeInBytes: Long,
  fileFormat: String  // "parquet", "orc", etc.
)

/**
 * Temporary credentials for accessing cloud storage.
 * Optional - only present when the server vends credentials.
 */
case class StorageCredentials(
  accessKeyId: String,
  secretAccessKey: String,
  sessionToken: String
)

/**
 * Result of a table scan plan operation.
 *
 * @param files List of files to scan
 * @param credentials Optional storage credentials. When present, these should be used
 *                    to access the files. When absent, credentials must be provided
 *                    through other means (e.g., IAM roles, environment variables).
 */
case class ScanPlan(
  files: Seq[ScanFile],
  credentials: Option[StorageCredentials] = None
)

/**
 * Interface for planning table scans via server-side planning (e.g., Iceberg REST catalog).
 * This interface is intentionally simple and has no dependencies
 * on Iceberg libraries, allowing it to live in delta-spark module.
 *
 * Note: Server-side planning only supports reading the current snapshot.
 */
trait ServerSidePlanningClient {
  /**
   * Plan a table scan and return the list of files to read.
   *
   * @param database The database or schema name
   * @param table The table name
   * @return ScanPlan containing files to read
   */
  def planScan(database: String, table: String): ScanPlan
}

/**
 * Factory for creating ServerSidePlanningClient instances.
 * This allows for configurable implementations (REST, mock, Spark-based, etc.)
 */
trait ServerSidePlanningClientFactory {
  /**
   * Create a client using metadata from catalog's loadTable.
   *
   * @param spark The SparkSession
   * @param metadata Metadata extracted from loadTable response
   * @return A ServerSidePlanningClient configured with the metadata
   */
  def buildFromMetadata(
      spark: SparkSession,
      metadata: ServerSidePlanningMetadata): ServerSidePlanningClient
}

/**
 * Registry for client factories. Can be configured for testing or to provide
 * production implementations (e.g., IcebergRESTCatalogPlanningClientFactory).
 *
 * By default, no factory is registered. Production code should register an appropriate
 * factory implementation before attempting to create clients.
 */
object ServerSidePlanningClientFactory {
  @volatile private var registeredFactory: Option[ServerSidePlanningClientFactory] = None

  /**
   * Set a factory for production use or testing.
   */
  def setFactory(factory: ServerSidePlanningClientFactory): Unit = {
    registeredFactory = Some(factory)
  }

  /**
   * Clear the registered factory.
   */
  def clearFactory(): Unit = {
    registeredFactory = None
  }

  /**
   * Get the currently registered factory.
   * Throws IllegalStateException if no factory has been registered.
   */
  def getFactory(): ServerSidePlanningClientFactory = {
    registeredFactory.getOrElse {
      throw new IllegalStateException(
        "No ServerSidePlanningClientFactory has been registered. " +
        "Call ServerSidePlanningClientFactory.setFactory() to register an implementation.")
    }
  }

  /**
   * Convenience method to create a client from metadata using the registered factory.
   */
  def buildFromMetadata(
      spark: SparkSession,
      metadata: ServerSidePlanningMetadata): ServerSidePlanningClient = {
    getFactory().buildFromMetadata(spark, metadata)
  }
}
