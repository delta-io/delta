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
import io.delta.storage.commit.uccommitcoordinator.{FixedUCTokenProvider, OAuthUCTokenProvider, UCClient, UCCommitCoordinatorClient, UCTokenBasedRestClient, UCTokenProvider}
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

  /** Suffix for the token configuration of a catalog. */
  final private val TOKEN_SUFFIX = "token"

  /** Suffix for the OAuth URI configuration of a catalog */
  final private val OAUTH_URI_SUFFIX = "oauthUri"

  /** Suffix for the OAuth client id configuration of a catalog */
  final private val OAUTH_CLIENT_ID_SUFFIX = "oauthClientId"

  /** Suffix for the OAuth client secret configuration of a catalog */
  final private val OAUTH_CLIENT_SECRET_SUFFIX = "oauthClientSecret"

  /** Cache for UCCommitCoordinatorClient instances. */
  private val commitCoordinatorClientCache =
    new ConcurrentHashMap[String, UCCommitCoordinatorClient]()

  // Helper cache for (uri, token) to metastoreId to avoid redundant calls to getMetastoreId
  // catalog.
  private val ucClientParamsToMetastoreIdCache = new ConcurrentHashMap[UCClientParams, String]()

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

  override def buildForCatalog(
      spark: SparkSession, catalogName: String): CommitCoordinatorClient = {
    val client = getCatalogConfigs(spark).find(_._1 == catalogName) match {
      case Some((_, ucClientParams)) => ucClientParams.buildUCClient()
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
    val matchingClients: List[UCClientParams] = getCatalogConfigs(spark)
      .map { case (_, ucClientParams: UCClientParams) => ucClientParams }
      .distinct // Remove duplicates since multiple catalogs can have the same uri and token
      .filter { case ucClientParams => getMetastoreId(ucClientParams).contains(metastoreId) }

    matchingClients match {
      case Nil => throw noMatchingCatalogException(metastoreId)
      case ucClientParams :: Nil => ucClientParams.buildUCClient()
      case multiple => throw multipleMatchingCatalogs(metastoreId, multiple.map(_.uri.get))
    }
  }

  /**
   * Retrieves the metastore ID for a given URI and token.
   *
   * This method creates a UCClient using the provided URI and token, then retrieves its metastore
   * ID. The result is cached to avoid unnecessary getMetastoreId requests in future calls. If
   * there's an error, it returns None and logs a warning.
   */
  private def getMetastoreId(ucClientParams: UCClientParams): Option[String] = {
    val uri = ucClientParams.uri.get
    try {
      val metastoreId = ucClientParamsToMetastoreIdCache.computeIfAbsent(
        ucClientParams,
        _ => {
          val ucClient = ucClientParams.buildUCClient()
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
  private[delta] def getCatalogConfigs(spark: SparkSession): List[(String, UCClientParams)] = {
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
        val oauthUri = catalogConfigs.get(
          s"$SPARK_SQL_CATALOG_PREFIX$catalogName.$OAUTH_URI_SUFFIX")
        val oauthClientId = catalogConfigs.get(
          s"$SPARK_SQL_CATALOG_PREFIX$catalogName.$OAUTH_CLIENT_ID_SUFFIX")
        val oauthClientSecret = catalogConfigs.get(
          s"$SPARK_SQL_CATALOG_PREFIX$catalogName.$OAUTH_CLIENT_SECRET_SUFFIX")

        UCClientParams.create(
          catalogName,
          uri,
          token,
          oauthUri,
          oauthClientId,
          oauthClientSecret) match {
          case Some(ucClientParams) =>
            Some(catalogName, ucClientParams)
          case _ =>
            None
        }
      }
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
    ucClientParamsToMetastoreIdCache.clear()
  }
}

trait UCClientFactory {
  def createUCClient(uri: String, token: String): UCClient =
    createUCClient(uri, new FixedUCTokenProvider(token))

  def createUCClient(uri: String, provider: UCTokenProvider): UCClient
}

object UCTokenBasedRestClientFactory extends UCClientFactory {
  override def createUCClient(uri: String, provider: UCTokenProvider): UCClient =
    new UCTokenBasedRestClient(uri, provider)
}

case class UCClientParams(
    uri: Option[String],
    token: Option[String] = None,
    oauthUri: Option[String] = None,
    oauthClientId: Option[String] = None,
    oauthClientSecret: Option[String] = None) {
  def buildUCClient(): UCClient = {
    (uri, token, oauthUri, oauthClientId, oauthClientSecret) match {
      case (Some(u), Some(t), _, _, _) =>
        UCTokenBasedRestClientFactory.createUCClient(u, t)
      case (Some(u), _, Some(oUri), Some(oClientId), Some(oClientSecret)) =>
        val provider = new OAuthUCTokenProvider(oUri, oClientId, oClientSecret)
        UCTokenBasedRestClientFactory.createUCClient(u, provider)
      case _ =>
        throw new IllegalStateException(
          "Invalid UCClientParams, missing token or oauth credentials")
    }
  }
}

object UCClientParams extends DeltaLogging {
  def create(
      catalogName: String,
      uri: Option[String],
      token: Option[String] = None,
      oauthUri: Option[String] = None,
      oauthClientId: Option[String] = None,
      oauthClientSecret: Option[String] = None): Option[UCClientParams] = {
    // Validate the uri.
    uri match {
      case Some(u) =>
        try {
          new URI(u)
        } catch {
          case _: URISyntaxException =>
            logWarning(log"Skipping catalog ${MDC(DeltaLogKeys.CATALOG, catalogName)} as it " +
              log"does not have a valid URI ${MDC(DeltaLogKeys.URI, u)}.")
        }
      case None => return None
    }

    (uri, token, oauthUri, oauthClientId, oauthClientSecret) match {
      case (Some(_), Some(_), _, _, _) =>
        // Use fixed token to build the UCClientParams.
        Some(UCClientParams(uri = uri, token = token))
      case (Some(_), _, Some(oUri), Some(_), Some(_)) =>
        // Validate the OAuth URI.
        try {
          new URI(oUri)
        } catch {
          case _: URISyntaxException =>
            logWarning(log"Skipping catalog ${MDC(DeltaLogKeys.CATALOG, catalogName)} " +
              log"as it does not have a valid OAuth URI")
            return None
        }
        // Use OAuth credentials to build the UCClientParams.
        Some(UCClientParams(
          uri = uri,
          oauthUri = oauthUri,
          oauthClientId = oauthClientId,
          oauthClientSecret = oauthClientSecret))
      case _ =>
        logWarning(log"Skipping catalog ${MDC(DeltaLogKeys.CATALOG, catalogName)} as it does " +
          "not have configured fixed token or oauth credential in Spark Session.")
        None
    }
  }
}
