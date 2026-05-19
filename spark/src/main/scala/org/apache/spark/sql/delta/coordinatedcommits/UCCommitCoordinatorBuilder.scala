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
import org.apache.spark.util.Utils

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

  /** The coordinator name used in table metadata to identify UC-managed tables. */
  final val COORDINATOR_NAME: String = "unity-catalog"

  /** Prefix for Spark SQL catalog configurations. */
  final private val SPARK_SQL_CATALOG_PREFIX = "spark.sql.catalog."

  /** Connector class name for filtering relevant Unity Catalog catalogs. */
  final private[delta] val UNITY_CATALOG_CONNECTOR_CLASS: String =
    "io.unitycatalog.spark.UCSingleCatalog"

  /** Cache for UCCommitCoordinatorClient instances. */
  private val commitCoordinatorClientCache =
    new ConcurrentHashMap[String, UCCommitCoordinatorClient]()

  // Cache for ucConfig to metastoreId to avoid redundant calls to getMetastoreId.
  private val ucConfigToMetastoreIdCache =
    new ConcurrentHashMap[Map[String, String], String]()

  // Use a var instead of val for ease of testing by injecting different UCClientFactory.
  private[delta] var ucClientFactory: UCClientFactory = UCTokenBasedRestClientFactory

  override def getName: String = COORDINATOR_NAME

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
      case Some((_, ucConfig)) => ucClientFactory.createUCClient(ucConfig)
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
    val matchingConfigs: List[Map[String, String]] = getCatalogConfigs(spark)
      .map(_._2)
      .distinct
      .filter { ucConfig => getMetastoreId(ucConfig).contains(metastoreId) }

    matchingConfigs match {
      case Nil => throw noMatchingCatalogException(metastoreId)
      case ucConfig :: Nil => ucClientFactory.createUCClient(ucConfig)
      case multiple =>
        throw multipleMatchingCatalogs(metastoreId, multiple.map(_.getOrElse("uri", "<unknown>")))
    }
  }

  /**
   * Retrieves the metastore ID for a given UC configuration map.
   *
   * This method creates a UCClient using the provided config, then retrieves its metastore ID.
   * The result is cached to avoid unnecessary getMetastoreId requests in future calls. If there's
   * an error, it returns None and logs a warning.
   */
  private def getMetastoreId(ucConfig: Map[String, String]): Option[String] = {
    try {
      val metastoreId = ucConfigToMetastoreIdCache.computeIfAbsent(
        ucConfig,
        _ => {
          val ucClient = ucClientFactory.createUCClient(ucConfig)
          try {
            ucClient.getMetastoreId
          } finally {
            safeClose(ucClient, ucConfig.getOrElse("uri", ""))
          }
        })
      Some(metastoreId)
    } catch {
      case NonFatal(e) =>
        logWarning(
          log"Failed to getMetastoreSummary with " +
            log"${MDC(DeltaLogKeys.URI, ucConfig.getOrElse("uri", ""))}",
          e)
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
   * For each Unity Catalog catalog, collects all sub-keys under
   * `spark.sql.catalog.<name>.*` (stripping the prefix) into a flat config map.
   * This includes `uri`, `auth.*`, `token` (legacy), `deltaRestApi.enabled`, and any
   * other catalog-specific settings.
   *
   * @return A list of (catalogName, ucConfig) for each properly configured UC catalog.
   */
  private[delta] def getCatalogConfigs(
      spark: SparkSession): List[(String, Map[String, String])] = {
    val allConfigs = spark.conf.getAll.filterKeys(_.startsWith(SPARK_SQL_CATALOG_PREFIX))

    // First, identify all Unity Catalog catalogs
    val ucCatalogNames = allConfigs
      .keys
      .map(_.split("\\."))
      .filter(_.length == 4)
      .map(_(3))
      .filter { catalogName: String =>
        val connector = allConfigs.get(s"$SPARK_SQL_CATALOG_PREFIX$catalogName")
        connector.contains(UNITY_CATALOG_CONNECTOR_CLASS)
      }

    // For each UC catalog, collect all sub-keys into a flat config map
    ucCatalogNames
      .flatMap { catalogName: String =>
        val prefix = s"$SPARK_SQL_CATALOG_PREFIX$catalogName."
        val ucConfig = allConfigs
          .filterKeys(_.startsWith(prefix))
          .map { case (k, v) => (k.stripPrefix(prefix), v) }
          .toMap

        ucConfig.get("uri") match {
          case None =>
            logWarning(
              log"Skipping catalog ${MDC(DeltaLogKeys.CATALOG, catalogName)} as it does " +
                "not have uri configured in Spark Session.")
            None
          case Some(uri) if !isValidUri(uri) =>
            logWarning(
              log"Skipping catalog ${MDC(DeltaLogKeys.CATALOG, catalogName)} as it has" +
                log" an invalid uri ${MDC(DeltaLogKeys.URI, uri)}.")
            None
          case _ =>
            Some((catalogName, ucConfig))
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
      case (name, ucConfig) => name -> UCCatalogConfig(name, ucConfig)
    }.toMap
  }

  private def isValidUri(uri: String): Boolean = {
    try { new URI(uri); true } catch { case _: URISyntaxException => false }
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
    ucConfigToMetastoreIdCache.clear()
  }
}

/** Factory trait for creating [[UCClient]] instances from a unified configuration map. */
trait UCClientFactory {
  def createUCClient(ucConfig: Map[String, String]): UCClient
}

/**
 * Default [[UCClientFactory]] that uses reflection to instantiate the [[UCClient]]
 * implementation so this module has no compile-time dependency on specific implementations
 * (e.g. UCDeltaTokenBasedRestClient).
 *
 * The `ucConfig` map is typically built from Spark catalog configuration. For example,
 * given these Spark configs:
 *
 * {{{
 *   spark.sql.catalog.my_catalog = io.unitycatalog.spark.UCSingleCatalog
 *   spark.sql.catalog.my_catalog.uri = https://my-uc-server.com
 *   spark.sql.catalog.my_catalog.auth.type = static
 *   spark.sql.catalog.my_catalog.auth.token = dapi1234567890
 * }}}
 *
 * The resulting `ucConfig` map (with the `spark.sql.catalog.my_catalog.` prefix stripped)
 * would be: `Map("uri" -> "...", "auth.type" -> "static", "auth.token" -> "...")`.
 *
 * Legacy format (token without auth. prefix) is also supported for backward compatibility:
 *
 * {{{
 *   spark.sql.catalog.my_catalog.uri = https://my-uc-server.com
 *   spark.sql.catalog.my_catalog.token = dapi1234567890
 * }}}
 *
 * Recognised ucConfig keys:
 *  - `uri` (required) -- the UC server endpoint.
 *  - `auth.*` / `token` (legacy) -- authentication parameters for [[TokenProvider]].
 *  - `deltaRestApi.enabled` -- if `"true"`, uses [[UCDeltaTokenBasedRestClient]];
 *    otherwise uses [[UCTokenBasedRestClient]].
 *  - `appVersions.*` -- caller-supplied version entries merged with defaults; e.g.
 *    `appVersions.Kernel -> "0.7.0"` adds a `"Kernel"` entry to the version map.
 */
object UCTokenBasedRestClientFactory extends UCClientFactory {

  final val URI_KEY = "uri"
  final val AUTH_PREFIX = "auth."
  final val DELTA_REST_API_ENABLED_KEY = "deltaRestApi.enabled"
  final val APP_VERSIONS_PREFIX = "appVersions."

  private val DEFAULT_UC_CLIENT_CLASS: String = classOf[UCTokenBasedRestClient].getName

  private val DELTA_UC_CLIENT_CLASS: String =
    "io.delta.storage.commit.uccommitcoordinator.UCDeltaTokenBasedRestClient"

  override def createUCClient(ucConfig: Map[String, String]): UCClient = {
    val uri = ucConfig.getOrElse(URI_KEY,
      throw new IllegalArgumentException(s"UC config must contain '$URI_KEY'"))

    val authConfig = extractAuthConfig(ucConfig)
    val tokenProvider = TokenProvider.create(authConfig.asJava)

    val className =
      if (ucConfig.get(DELTA_REST_API_ENABLED_KEY).exists(_.equalsIgnoreCase("true"))) {
        DELTA_UC_CLIENT_CLASS
      } else {
        DEFAULT_UC_CLIENT_CLASS
      }

    val cls = Utils.classForName(className)
    require(classOf[UCClient].isAssignableFrom(cls),
      s"$className does not implement ${classOf[UCClient].getName}")
    val appVersions = extractAppVersions(ucConfig)
    val ctor = cls.getConstructor(
      classOf[String], classOf[TokenProvider], classOf[java.util.Map[_, _]])
    ctor.newInstance(uri, tokenProvider, appVersions.asJava).asInstanceOf[UCClient]
  }

  /** Java-friendly overload that accepts a java.util.Map. */
  def createUCClient(ucConfig: java.util.Map[String, String]): UCClient = {
    createUCClient(ucConfig.asScala.toMap)
  }

  /**
   * Extracts authentication configuration from ucConfig.
   * Prefers `auth.*` keys; falls back to legacy `token` key.
   */
  private[coordinatedcommits] def extractAuthConfig(
      ucConfig: Map[String, String]): Map[String, String] = {
    val authConfig = ucConfig
      .filterKeys(_.startsWith(AUTH_PREFIX))
      .map { case (k, v) => (k.stripPrefix(AUTH_PREFIX), v) }
      .toMap

    if (authConfig.nonEmpty) {
      authConfig
    } else {
      ucConfig.get("token") match {
        case Some(token) => Map("type" -> "static", "token" -> token)
        case None => Map.empty
      }
    }
  }

  /**
   * Merges default app versions with any `appVersions.*` entries from ucConfig.
   * Caller-supplied entries override defaults with the same key.
   */
  private[coordinatedcommits] def extractAppVersions(
      ucConfig: Map[String, String]): Map[String, String] = {
    val extra = ucConfig
      .filterKeys(_.startsWith(APP_VERSIONS_PREFIX))
      .map { case (k, v) => (k.stripPrefix(APP_VERSIONS_PREFIX), v) }
      .toMap
    defaultAppVersions ++ extra
  }

  private[coordinatedcommits] def defaultAppVersions: Map[String, String] = {
    Map(
      "Delta" -> io.delta.VERSION,
      "Spark" -> org.apache.spark.SPARK_VERSION,
      "Scala" -> scala.util.Properties.versionNumberString,
      "Java" -> System.getProperty("java.version")
    )
  }
}

/**
 * Holder for Unity Catalog configuration extracted from Spark configs.
 * The `ucConfig` map contains all sub-keys under `spark.sql.catalog.<name>.*`
 * with the prefix stripped.
 * Used by [[UCCommitCoordinatorBuilder.getCatalogConfigMap]].
 */
case class UCCatalogConfig(catalogName: String, ucConfig: Map[String, String]) {

  def uri: String = ucConfig.getOrElse("uri",
    throw new NoSuchElementException(s"No URI in config for catalog $catalogName"))

  /**
   * Returns the authentication config suitable for [[TokenProvider.create]].
   * Prefers `auth.*` keys; falls back to legacy `token` key.
   */
  def authConfig: Map[String, String] =
    UCTokenBasedRestClientFactory.extractAuthConfig(ucConfig)
}
