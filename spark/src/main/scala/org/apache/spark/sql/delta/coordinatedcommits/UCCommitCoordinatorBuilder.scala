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

import io.delta.storage.commit.CommitCoordinatorClient
import io.delta.storage.commit.uccommitcoordinator.{UCClient, UCCommitCoordinatorClient, UCTokenBasedRestClient}

import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging

import io.unitycatalog.client.auth.TokenProvider
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
object UCCommitCoordinatorBuilder
    extends CatalogOwnedCommitCoordinatorBuilder with DeltaLogging {

  /** Prefix for Spark SQL catalog configurations. */
  final private val SPARK_SQL_CATALOG_PREFIX = "spark.sql.catalog."

  /** Connector class name for filtering relevant Unity Catalog catalogs. */
  final private[delta] val UNITY_CATALOG_CONNECTOR_CLASS: String =
    "io.unitycatalog.spark.UCSingleCatalog"

  /** Suffix for the URI configuration of a catalog. */
  final private val URI_SUFFIX = "uri"

  /** Cache for UCCommitCoordinatorClient instances. */
  private val commitCoordinatorClientCache =
    new ConcurrentHashMap[String, UCCommitCoordinatorClient]()

  // Helper cache for (uri, authConfig) to metastoreId to avoid redundant calls to getMetastoreId
  private val uriAuthConfigToMetastoreIdCache =
    new ConcurrentHashMap[(String, Map[String, String]), String]()

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
      _ => new UCCommitCoordinatorClient(conf.asJava, getMatchingUCClient(spark, metastoreId)))
  }

  override def buildForCatalog(
      spark: SparkSession,
      catalogName: String): CommitCoordinatorClient = {
    val client = getCatalogConfigs(spark).find(_._1 == catalogName) match {
      case Some((_, uri, authConfig)) => ucClientFactory.createUCClient(uri, authConfig)
      case None =>
        throw new IllegalArgumentException(
          s"Catalog $catalogName not found in the provided SparkSession configurations.")
    }
    val conf = Map.empty[String, String]
    new UCCommitCoordinatorClient(conf.asJava, client)
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
    val matchingClients: List[(String, Map[String, String])] = getCatalogConfigs(spark)
      .map { case (name, uri, authConfig) => (uri, authConfig) }
      .distinct // Remove duplicates since multiple catalogs can have the same uri and config
      .filter { case (uri, authConfig) => getMetastoreId(uri, authConfig).contains(metastoreId) }

    matchingClients match {
      case Nil => throw noMatchingCatalogException(metastoreId)
      case (uri, authConfig) :: Nil => ucClientFactory.createUCClient(uri, authConfig)
      case multiple => throw multipleMatchingCatalogs(metastoreId, multiple.map(_._1))
    }
  }

  /**
   * Retrieves the metastore ID for a given URI and auth configuration map.
   *
   * This method creates a UCClient using the provided URI and auth configuration map, then
   * retrieves its metastore ID. The result is cached to avoid unnecessary getMetastoreId requests
   * in future calls. If there's an error, it returns None and logs a warning.
   */
  private def getMetastoreId(uri: String, authConfig: Map[String, String]): Option[String] = {
    try {
      val metastoreId = uriAuthConfigToMetastoreIdCache.computeIfAbsent(
        (uri, authConfig),
        _ => {
          val ucClient = ucClientFactory.createUCClient(uri, authConfig)
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
        "any required Unity Catalog authentication configurations. " +
        "Note that the matching process involves retrieving the metastoreId using the " +
        "provided configuration in Spark Session configs.")
  }

  private def multipleMatchingCatalogs(metastoreId: String, uris: List[String]) = {
    new IllegalStateException(
      s"Found multiple catalogs for UC metastore ID $metastoreId at $uris. " +
        "Please ensure the catalog is configured correctly by setting " +
        "`spark.sql.catalog.<catalog-name>`, `spark.sql.catalog.<catalog-name>.uri` and " +
        "any required Unity Catalog authentication configurations. " +
        "Note that the matching process involves retrieving the metastoreId using the " +
        "provided configuration in Spark Session configs.")
  }

  /**
   * Retrieves the catalog configurations from the SparkSession.
   *
   * This method supports both the new auth.* format and the legacy token format for backward
   * compatibility:
   *
   * New format:
   *   spark.sql.catalog.catalog1.uri = "https://dbc-123abc.databricks.com"
   *   spark.sql.catalog.catalog1.auth.type = "static"
   *   spark.sql.catalog.catalog1.auth.token = "dapi1234567890"
   *
   * Legacy format (for backward compatibility):
   *   spark.sql.catalog.catalog1.uri = "https://dbc-123abc.databricks.com"
   *   spark.sql.catalog.catalog1.token = "dapi1234567890"
   *
   * When the legacy format is detected (token without auth. prefix), it is automatically
   * converted to the new format (type=static, token=value) for TokenProvider.
   *
   * @return
   *   A list of tuples containing (catalogName, uri, authConfigMap) for each properly configured
   *   catalog. The authConfigMap contains authentication configurations ready to be passed to
   *   TokenProvider.create().
   */
  private[delta] def getCatalogConfigs(
      spark: SparkSession): List[(String, String, Map[String, String])] = {
    val catalogConfigs = spark.conf.getAll.filterKeys(_.startsWith(SPARK_SQL_CATALOG_PREFIX))

    // First, identify all Unity Catalog catalogs
    val ucCatalogNames = catalogConfigs
      .keys
      .map(_.split("\\."))
      .filter(_.length == 4)
      .map(_(3))
      .filter { catalogName: String =>
        val connector = catalogConfigs.get(s"$SPARK_SQL_CATALOG_PREFIX$catalogName")
        connector.contains(UNITY_CATALOG_CONNECTOR_CLASS)
      }

    // For each UC catalog, extract its URI and auth configurations
    ucCatalogNames
      .flatMap { catalogName: String =>
        val catalogPrefix = s"$SPARK_SQL_CATALOG_PREFIX$catalogName."
        val authPrefix = s"${catalogPrefix}auth."
        val uriOpt = catalogConfigs.get(s"$catalogPrefix$URI_SUFFIX")

        uriOpt match {
          case Some(uri) =>
            try {
              new URI(uri) // Validate the URI

              // Extract all auth.* configuration keys for this catalog
              // and strip the "spark.sql.catalog.<catalog-name>.auth." prefix
              var authConfigMap = catalogConfigs
                .filterKeys(_.startsWith(authPrefix))
                .map { case (fullKey, value) =>
                  // Remove the auth prefix to get just the auth config key
                  // e.g., "spark.sql.catalog.catalog1.auth.type" -> "type"
                  // e.g., "spark.sql.catalog.catalog1.auth.oauth.uri" -> "oauth.uri"
                  val authKey = fullKey.stripPrefix(authPrefix)
                  (authKey, value)
                }
                .toMap

              // Support legacy format: if no auth.* configs but token exists,
              // convert to new format (type=static, token=value)
              if (authConfigMap.isEmpty) {
                val legacyTokenOpt = catalogConfigs.get(s"${catalogPrefix}token")
                legacyTokenOpt match {
                  case Some(token) =>
                    authConfigMap = Map("type" -> "static", "token" -> token)
                  case None =>
                  // No auth configs found
                }
              }

              if (authConfigMap.isEmpty) {
                logWarning(
                  log"Skipping catalog ${MDC(DeltaLogKeys.CATALOG, catalogName)} as it " +
                    "does not have any authentication configurations in Spark Session.")
                None
              } else {
                Some((catalogName, uri, authConfigMap))
              }
            } catch {
              case _: URISyntaxException =>
                logWarning(
                  log"Skipping catalog ${MDC(DeltaLogKeys.CATALOG, catalogName)} as it " +
                    log"does not have a valid URI ${MDC(DeltaLogKeys.URI, uri)}.")
                None
            }
          case None =>
            logWarning(
              log"Skipping catalog ${MDC(DeltaLogKeys.CATALOG, catalogName)} as it does " +
                "not have uri configured in Spark Session.")
            None
        }
      }
      .toList
  }

  /**
   * Returns catalog configurations as a Map for O(1) lookup by catalog name.
   * Wraps [[getCatalogConfigs]] results in [[UCCatalogConfig]] for better readability.
   */
  private[delta] def getCatalogConfigMap(spark: SparkSession): Map[String, UCCatalogConfig] = {
    getCatalogConfigs(spark).map {
      case (name, uri, authConfig) => name -> UCCatalogConfig(name, uri, authConfig)
    }.toMap
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
    uriAuthConfigToMetastoreIdCache.clear()
  }
}

trait UCClientFactory {
  def createUCClient(uri: String, authConfig: Map[String, String]): UCClient
}

object UCTokenBasedRestClientFactory extends UCClientFactory {
  override def createUCClient(uri: String, authConfig: Map[String, String]): UCClient = {
    // Create TokenProvider from the authentication configuration map
    // We pass the configuration through without interpreting any specific keys,
    // as those are managed by the Unity Catalog client library
    val tokenProvider = TokenProvider.create(authConfig.asJava)
    new UCTokenBasedRestClient(uri, tokenProvider)
  }
}

/**
 * Holder for Unity Catalog configuration extracted from Spark configs.
 * Used by [[UCCommitCoordinatorBuilder.getCatalogConfigMap]].
 */
case class UCCatalogConfig(catalogName: String, uri: String, authConfig: Map[String, String])
