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

package org.apache.spark.sql.delta.coordinatedcommits

import java.net.{URI, URISyntaxException}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import io.delta.storage.commit.CommitCoordinatorClient
import io.delta.storage.commit.uccommitcoordinator.{UCClient, UCCommitCoordinatorClient, UCTokenBasedRestClient}

import org.apache.spark.internal.MDC
import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession

/**
 * Builder for Unity Catalog Commit Coordinator Clients.
 *
 * This builder is responsible for creating and caching UCCommitCoordinatorClient instances
 * based on the provided metastore IDs and catalog configurations.
 *
 * It caches the UCCommitCoordinatorClient instance for a given metastore ID upon its first access.
 */
object UCCommitCoordinatorBuilder extends CommitCoordinatorBuilder with DeltaLogging {

  /** Prefix for Spark SQL catalog configurations. */
  final private val SPARK_SQL_CATALOG_PREFIX = "spark.sql.catalog."

  /** Connector class name for filtering relevant Unity Catalog catalogs. */
  final private val UNITY_CATALOG_CONNECTOR_CLASS: String =
    "io.unitycatalog.connectors.spark.UCSingleCatalog"

  /** Suffix for the URI configuration of a catalog. */
  final private val URI_SUFFIX = "uri"

  /** Suffix for the token configuration of a catalog. */
  final private val TOKEN_SUFFIX = "token"

  /** Cache for UCCommitCoordinatorClient instances. */
  private val commitCoordinatorClientCache =
    new ConcurrentHashMap[String, UCCommitCoordinatorClient]()

  // Helper cache for (uri, token) to metastoreId to avoid redundant calls to getMetastoreId
  // catalog.
  private val uriTokenToMetastoreIdCache = new ConcurrentHashMap[(String, String), String]()

  // Use a var instead of val for ease of testing by injecting different UCClientFactory.
  private[delta] var ucClientFactory: UCClientFactory = UCTokenBasedRestClientFactory

  override def getName: String = "unity-catalog"

  override def build(spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient = {
    val metastoreId = conf.getOrElse(
      UCCommitCoordinatorClient.UC_METASTORE_ID_KEY,
      throw new IllegalArgumentException(
        s"UC metastore ID not found in the provided coordinator conf: $conf"))

    commitCoordinatorClientCache.computeIfAbsent(
      metastoreId,
      _ => new UCCommitCoordinatorClient(conf.asJava, getMatchingUCClient(spark, metastoreId))
    )
  }

  /**
   * Finds and returns a UCClient that matches the given metastore ID.
   *
   * This method iterates through all configured catalogs in SparkSession, creates UCClients for
   * each, gets their metastore ID and returns the one that matches the provided metastore ID.
   * If no matching catalog is found or if multiple matching catalogs are found, it throws an
   * appropriate exception.
   */
  private def getMatchingUCClient(spark: SparkSession, metastoreId: String): UCClient = {
    val matchingClients: List[(String, String)] = getCatalogConfigs(spark)
      .map { case (name, uri, token) => (uri, token) }
      .distinct // Remove duplicates since multiple catalogs can have the same uri and token
      .filter { case (uri, token) => getMetastoreId(uri, token).contains(metastoreId) }

    matchingClients match {
      case Nil => throw noMatchingCatalogException(metastoreId)
      case (uri, token) :: Nil => ucClientFactory.createUCClient(uri, token)
      case multiple => throw multipleMatchingCatalogs(metastoreId, multiple.map(_._1))
    }
  }

  /**
   * Retrieves the metastore ID for a given URI and token.
   *
   * This method creates a UCClient using the provided URI and token, then retrieves its metastore
   * ID. The result is cached to avoid unnecessary getMetastoreId requests in future calls. If
   * there's an error, it returns None and logs a warning.
   */
  private def getMetastoreId(uri: String, token: String): Option[String] = {
    try {
      val metastoreId = uriTokenToMetastoreIdCache.computeIfAbsent(
        (uri, token),
        _ => {
          val ucClient = ucClientFactory.createUCClient(uri, token)
          try {
            ucClient.getMetastoreId
          } finally {
            safeClose(ucClient, uri)
          }
        })
      Some(metastoreId)
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to getMetastoreSummary with ${MDC(DeltaLogKeys.URI, uri)}", e)
        None
    }
  }

  private def noMatchingCatalogException(metastoreId: String) = {
    new IllegalStateException(
      s"No matching catalog found for UC metastore ID $metastoreId. " +
        "Please ensure the catalog is configured correctly by setting " +
        "`spark.sql.catalog.<catalog-name>`, `spark.sql.catalog.<catalog-name>.uri` and " +
        "`spark.sql.catalog.<catalog-name>.token`. Note that the matching process involves " +
        "retrieving the metastoreId using the provided `<uri, token>` pairs in Spark " +
        "Session configs.")
  }

  private def multipleMatchingCatalogs(metastoreId: String, uris: List[String]) = {
    new IllegalStateException(
      s"Found multiple catalogs for UC metastore ID $metastoreId at $uris. " +
        "Please ensure the catalog is configured correctly by setting " +
        "`spark.sql.catalog.<catalog-name>`, `spark.sql.catalog.<catalog-name>.uri` and " +
        "`spark.sql.catalog.<catalog-name>.token`. Note that the matching process involves " +
        "retrieving the metastoreId using the provided `<uri, token>` pairs in Spark " +
        "Session configs.")
  }

  /**
   * Retrieves the catalog configurations from the SparkSession.
   *
   * Example; Given Spark configurations:
   *   spark.sql.catalog.catalog1 = "io.unitycatalog.connectors.spark.UCSingleCatalog"
   *   spark.sql.catalog.catalog1.uri = "https://dbc-123abc.databricks.com"
   *   spark.sql.catalog.catalog1.token = "dapi1234567890"
   *
   *   spark.sql.catalog.catalog2 = "io.unitycatalog.connectors.spark.UCSingleCatalog"
   *   spark.sql.catalog.catalog2.uri = "https://dbc-456def.databricks.com"
   *   spark.sql.catalog.catalog2.token = "dapi0987654321"
   *
   *   spark.sql.catalog.catalog3 = "io.unitycatalog.connectors.spark.UCSingleCatalog"
   *   spark.sql.catalog.catalog3.uri = "https://dbc-789ghi.databricks.com"
   *
   *   spark.sql.catalog.catalog4 = "com.databricks.sql.lakehouse.catalog3"
   *   spark.sql.catalog.catalog4.uri = "https://dbc-456def.databricks.com"
   *   spark.sql.catalog.catalog4.token = "dapi0987654321"
   *
   *   spark.sql.catalog.catalog5 = "io.unitycatalog.connectors.spark.UCSingleCatalog"
   *   spark.sql.catalog.catalog5.uri = "random-string"
   *   spark.sql.catalog.catalog5.token = "dapi0987654321"
   *
   * This method would return:
   * List(
   *   ("catalog1", "https://dbc-123abc.databricks.com", "dapi1234567890"),
   *   ("catalog2", "https://dbc-456def.databricks.com", "dapi0987654321")
   * )
   *
   * Note: catalog3 is not included in the result because it's missing the token configuration.
   * Note: catalog4 is not included in the result because it's not a UCSingleCatalog connector.
   * Note: catalog5 is not included in the result because its URI is not a valid URI.
   *
   * @return
   *   A list of tuples containing (catalogName, uri, token) for each properly configured catalog
   */
  private[delta] def getCatalogConfigs(spark: SparkSession): List[(String, String, String)] = {
    val catalogConfigs = spark.conf.getAll.filterKeys(_.startsWith(SPARK_SQL_CATALOG_PREFIX))

    catalogConfigs
      .keys
      .map(_.split("\\."))
      .filter(_.length == 4)
      .map(_(3))
      .filter { catalogName: String =>
        val connector = catalogConfigs.get(s"$SPARK_SQL_CATALOG_PREFIX$catalogName")
        connector.contains(UNITY_CATALOG_CONNECTOR_CLASS)}
      .flatMap { catalogName: String =>
        val uri = catalogConfigs.get(s"$SPARK_SQL_CATALOG_PREFIX$catalogName.$URI_SUFFIX")
        val token = catalogConfigs.get(s"$SPARK_SQL_CATALOG_PREFIX$catalogName.$TOKEN_SUFFIX")
        (uri, token) match {
          case (Some(u), Some(t)) =>
            try {
              new URI(u) // Validate the URI
              Some((catalogName, u, t))
            } catch {
              case _: URISyntaxException =>
                logWarning(log"Skipping catalog ${MDC(DeltaLogKeys.CATALOG, catalogName)} as it " +
                  log"does not have a valid URI ${MDC(DeltaLogKeys.URI, u)}.")
                None
            }
          case _ =>
            logWarning(log"Skipping catalog ${MDC(DeltaLogKeys.CATALOG, catalogName)} as it does " +
              "not have both uri and token configured in Spark Session.")
            None
        }}
      .toList
  }

  private def safeClose(ucClient: UCClient, uri: String): Unit = {
    try {
      ucClient.close()
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to close UCClient for uri ${MDC(DeltaLogKeys.URI, uri)}", e)
    }
  }

  def clearCache(): Unit = {
    commitCoordinatorClientCache.clear()
    uriTokenToMetastoreIdCache.clear()
  }
}

trait UCClientFactory {
  def createUCClient(uri: String, token: String): UCClient
}

object UCTokenBasedRestClientFactory extends UCClientFactory {
  override def createUCClient(uri: String, token: String): UCClient =
    new UCTokenBasedRestClient(uri, token)
}
