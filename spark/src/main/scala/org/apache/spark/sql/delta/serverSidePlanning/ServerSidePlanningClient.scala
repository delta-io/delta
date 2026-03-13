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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.Filter

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
 * Interface for planning table scans via server-side planning.
 * This interface uses Spark's standard `org.apache.spark.sql.sources.Filter` as the universal
 * representation for filter pushdown. This keeps the interface catalog-agnostic while allowing
 * each server-side planning catalog implementation to convert filters to their own native format.
 */
trait ServerSidePlanningClient {
  /**
   * Plan a table scan and return the list of files to read.
   *
   * @param databaseName The database or schema name
   * @param table The table name
   * @param filterOption Optional filter expression to push down to server (Spark Filter format)
   * @param projectionOption Optional projection (column names) to push down to server
   * @param limitOption Optional limit to push down to server
   * @return ScanPlan containing files to read
   */
  def planScan(
      databaseName: String,
      table: String,
      filterOption: Option[Filter] = None,
      projectionOption: Option[Seq[String]] = None,
      limitOption: Option[Int] = None): ScanPlan

  /**
   * Check if all given filters can be converted to the server's native filter format.
   * This is used during filter pushdown to determine whether to return residuals to Spark.
   *
   * @param filters Array of Spark filters to check
   * @return true if ALL filters can be converted, false if ANY filter cannot be converted
   */
  def canConvertFilters(filters: Array[Filter]): Boolean
}

/**
 * Factory for creating ServerSidePlanningClient instances.
 * This allows for configurable implementations (REST, mock, Spark-based, etc.)
 */
private[serverSidePlanning] trait ServerSidePlanningClientFactory {
  /**
   * Create a client using metadata necessary for server-side planning.
   *
   * @param spark The SparkSession
   * @param metadata Metadata necessary for server-side planning
   * @return A ServerSidePlanningClient configured with the metadata
   */
  def buildClient(
      spark: SparkSession,
      metadata: ServerSidePlanningMetadata): ServerSidePlanningClient
}

/**
 * Registry for client factories. Automatically discovers and registers implementations
 * using reflection-based auto-discovery on first access to the factory. Manual registration
 * using setFactory() is only needed for testing or to override the auto-discovered factory.
 */
private[serverSidePlanning] object ServerSidePlanningClientFactory {
  // Fully qualified class name for auto-registration via reflection
  private val ICEBERG_FACTORY_CLASS_NAME =
    "org.apache.spark.sql.delta.serverSidePlanning.IcebergRESTCatalogPlanningClientFactory"

  @volatile private var registeredFactory: Option[ServerSidePlanningClientFactory] = None
  @volatile private var autoRegistrationAttempted: Boolean = false

  // Lazy initialization - only runs when getFactory() is called and no factory is set.
  // Uses reflection to load the hardcoded IcebergRESTCatalogPlanningClientFactory class.
  private def tryAutoRegisterFactory(): Unit = {
    // Double-checked locking pattern to ensure initialization happens only once
    if (!autoRegistrationAttempted) {
      synchronized {
        if (!autoRegistrationAttempted) {
          autoRegistrationAttempted = true

          try {
            // Use reflection to load the Iceberg factory class
            // scalastyle:off classforname
            val clazz = Class.forName(ICEBERG_FACTORY_CLASS_NAME)
            // scalastyle:on classforname
            val factory = clazz.getConstructor().newInstance()
              .asInstanceOf[ServerSidePlanningClientFactory]
            registeredFactory = Some(factory)
          } catch {
            case e: Exception =>
              throw new IllegalStateException(
                "No ServerSidePlanningClientFactory has been registered. " +
                "Ensure delta-iceberg JAR is on the classpath for auto-registration, " +
                "or call ServerSidePlanningClientFactory.setFactory() to register manually.",
                e)
          }
        }
      }
    }
  }

  /**
   * Set a factory, overriding any auto-registered factory.
   * Synchronized to prevent race conditions with auto-registration.
   */
  private[serverSidePlanning] def setFactory(factory: ServerSidePlanningClientFactory): Unit = {
    synchronized {
      registeredFactory = Some(factory)
    }
  }

  /**
   * Clear the registered factory.
   * Synchronized to ensure atomic reset of both flags.
   */
  private[serverSidePlanning] def clearFactory(): Unit = {
    synchronized {
      registeredFactory = None
      autoRegistrationAttempted = false
    }
  }

  /**
   * Get the currently registered factory.
   * Throws IllegalStateException if no factory has been registered (either via reflection-based
   * auto-discovery or explicit setFactory() call).
   */
  def getFactory(): ServerSidePlanningClientFactory = {
    // Try auto-registration if not already attempted and no factory is manually set
    if (registeredFactory.isEmpty) {
      tryAutoRegisterFactory()
    }

    registeredFactory.getOrElse {
      throw new IllegalStateException(
        "No ServerSidePlanningClientFactory has been registered. " +
        "Ensure delta-iceberg JAR is on the classpath for auto-registration, " +
        "or call ServerSidePlanningClientFactory.setFactory() to register manually.")
    }
  }

  /**
   * Convenience method to create a client from metadata using the registered factory.
   */
  def buildClient(
      spark: SparkSession,
      metadata: ServerSidePlanningMetadata): ServerSidePlanningClient = {
    getFactory().buildClient(spark, metadata)
  }
}

/**
 * Functional interface for applying storage credentials to a Hadoop configuration.
 * Implementations are responsible for setting the appropriate Hadoop config keys
 * for their respective cloud provider.
 */
trait ScanPlanStorageCredentials {
  def configure(conf: org.apache.hadoop.conf.Configuration): Unit
}

/**
 * Result of a table scan plan operation.
 */
case class ScanPlan(
    files: Seq[ScanFile],
    credentials: Option[ScanPlanStorageCredentials] = None)
