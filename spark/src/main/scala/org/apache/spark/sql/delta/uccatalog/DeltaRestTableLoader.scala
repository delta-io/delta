/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.uccatalog

import scala.collection.JavaConverters._

import io.delta.storage.commit.uccommitcoordinator.UCDeltaClient
import io.unitycatalog.client.delta.model.{
  CredentialOperation,
  CredentialsResponse,
  LoadTableResponse,
  StorageCredential,
  TableType => UCTableType
}

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog.{Identifier, V1Table}

/**
 * Builds a Spark `V1Table` from a Delta REST Catalog (DRC) `LoadTableResponse`. Mirrors the
 * shape of what `UCProxy.loadTable` produces today so that downstream code in
 * `AbstractDeltaCatalog.loadTable` (`DeltaTableUtils.isDeltaTable`, `loadCatalogTable`, ...)
 * does not need to change.
 *
 * On the read path, vended storage credentials are fetched with READ scope and injected into
 * `CatalogTable.storage.properties` under the `io.unitycatalog.drc.cred.` prefix so that
 * downstream file IO uses the per-table, per-operation principal. They are intentionally NOT
 * written to the `SparkSession.conf`, so concurrent queries with different principals do not
 * clobber each other in a shared cluster -- a known footgun called out in review.
 */
private[delta] object DeltaRestTableLoader {

  /** Stashed on `CatalogTable.properties` so the commit path (later PR) can assert-etag. */
  val PROP_DRC_ETAG: String = "io.unitycatalog.drc.etag"
  /** Stashed on `CatalogTable.properties` so the commit path can assert-table-uuid. */
  val PROP_DRC_TABLE_ID: String = "io.unitycatalog.drc.table-id"
  /** Latest table version per DRC response -- informational. */
  val PROP_DRC_LATEST_VERSION: String = "io.unitycatalog.drc.latest-table-version"

  /**
   * Prefix for DRC-vended storage credential properties on `CatalogTable.storage.properties`.
   * The raw config keys from the DRC `StorageCredential.config` map (e.g. `s3.access-key-id`,
   * `azure.sas-token`) are written as `{PROP_DRC_CREDENTIAL_PREFIX}{raw-key}`. Downstream
   * Hadoop FS binding (converting these to `fs.s3a.access.key` etc.) is handled by the layer
   * that materializes the options into a Hadoop Configuration for table-scoped IO.
   */
  val PROP_DRC_CREDENTIAL_PREFIX: String = "io.unitycatalog.drc.cred."

  /**
   * Loads a table via the DRC client and assembles a `V1Table`. Also fetches READ-scope
   * credentials and injects them into `CatalogTable.storage.properties`. If the credentials
   * endpoint errors, the table still loads without credentials (the server-side implementation
   * can lag the client in v1).
   *
   * @param catalogName the Spark-side catalog name (e.g. "unity"); becomes the catalog segment
   *                    of the returned `TableIdentifier`.
   * @param ident the table identifier -- namespace must contain exactly the schema name.
   * @param client the DRC client (one is constructed per load; shares the UC-owned ApiClient).
   */
  def load(catalogName: String, ident: Identifier, client: UCDeltaClient): V1Table = {
    require(catalogName != null && catalogName.nonEmpty, "catalogName must be non-empty")
    require(ident != null, "ident must not be null")
    require(client != null, "client must not be null")
    require(ident.namespace() != null && ident.namespace().length == 1,
      s"DRC only supports single-level namespaces, got: ${ident}")

    val schemaName = ident.namespace()(0)
    val tableName = ident.name()
    val response: LoadTableResponse = client.loadTable(catalogName, schemaName, tableName)

    // Fetch vended READ-scope credentials. If the server-side creds endpoint errors (the
    // DRC server implementation lags the client in v1), the table still loads without
    // creds -- file IO falls back to whatever the executor's Hadoop conf already provides.
    // We do NOT fall back silently on loadTable errors above: only credentials.
    val credentials: Option[CredentialsResponse] =
      try Some(client.getTableCredentials(
        catalogName, schemaName, tableName, CredentialOperation.READ))
      catch {
        case _: java.io.IOException => None
      }

    buildV1Table(catalogName, ident, response, credentials)
  }

  /** Visible for testing. */
  private[uccatalog] def buildV1Table(
      catalogName: String,
      ident: Identifier,
      response: LoadTableResponse,
      credentials: Option[CredentialsResponse] = None): V1Table = {
    require(response != null, "DRC loadTable response must not be null")
    val md = Option(response.getMetadata).getOrElse(
      throw new IllegalStateException("DRC loadTable response is missing metadata"))

    val columns = Option(md.getColumns)
      .map(_.getFields)
      .getOrElse(java.util.Collections.emptyList())
    val sparkSchema = DeltaRestSchemaConverter.toSparkSchema(columns)

    val schemaName = ident.namespace()(0)
    val tableIdentifier = TableIdentifier(ident.name(), Some(schemaName), Some(catalogName))

    val locationUri = Option(md.getLocation).map(CatalogUtils.stringToURI)
    val properties = Option(md.getProperties)
      .map(_.asScala.toMap)
      .getOrElse(Map.empty[String, String])

    val credentialProps = credentials
      .flatMap(r => Option(r.getStorageCredentials).map(_.asScala.toSeq))
      .flatMap(selectLongestPrefixMatch(_, Option(md.getLocation).getOrElse("")))
      .map(credentialToStorageProps)
      .getOrElse(Map.empty[String, String])

    val enrichedProps = properties ++ Map(
      PROP_DRC_ETAG -> Option(md.getEtag).getOrElse(""),
      PROP_DRC_TABLE_ID -> Option(md.getTableUuid).map(_.toString).getOrElse(""),
      PROP_DRC_LATEST_VERSION ->
        Option(response.getLatestTableVersion).map(_.toString).getOrElse("")) ++ credentialProps

    val tableType = md.getTableType match {
      case UCTableType.MANAGED => CatalogTableType.MANAGED
      case _ => CatalogTableType.EXTERNAL
    }

    val partitionCols = Option(md.getPartitionColumns)
      .map(_.asScala.toSeq)
      .getOrElse(Nil)

    val catalogTable = CatalogTable(
      identifier = tableIdentifier,
      tableType = tableType,
      storage = CatalogStorageFormat.empty.copy(locationUri = locationUri),
      schema = sparkSchema,
      provider = Some("delta"),
      partitionColumnNames = partitionCols,
      properties = enrichedProps
    )
    V1Table(catalogTable)
  }

  /**
   * Picks the single best-matching `StorageCredential` for the given table location. The match
   * rule is "longest prefix wins" among credentials whose normalized prefix is a prefix of the
   * normalized table location. Scheme aliasing (`s3a://` <-> `s3://`, `abfss://` <-> `abfs://`)
   * is handled by `normalizeLocation`. Returns `None` if no credential applies.
   *
   * This is the guard against the scope-leak silent failure: a future regression that uses a
   * raw `startsWith` with first-match-wins iteration order would attach an overly broad
   * `s3://bucket/` credential to a `s3://bucket/tenant-a/secret/` table.
   */
  private[uccatalog] def selectLongestPrefixMatch(
      creds: Seq[StorageCredential],
      tableLocation: String): Option[StorageCredential] = {
    if (creds.isEmpty) return None
    val normLoc = normalizeLocation(tableLocation)
    creds
      .flatMap { cred =>
        val raw = Option(cred.getPrefix).getOrElse("")
        val normPrefix = normalizeLocation(raw)
        if (normPrefix.isEmpty || normLoc.isEmpty) {
          // An empty prefix credential matches everything; an empty location means no table
          // location, so loosen to match. The .length tiebreak still prefers specific
          // credentials when multiple apply.
          Some((cred, normPrefix.length))
        } else if (normLoc == normPrefix
            || normLoc.startsWith(normPrefix + "/")
            || normLoc.startsWith(normPrefix)) {
          Some((cred, normPrefix.length))
        } else {
          None
        }
      }
      .sortBy(-_._2) // longest prefix first
      .headOption
      .map(_._1)
  }

  /**
   * Normalizes a cloud storage location for prefix comparison: lower-cases the scheme,
   * collapses alias schemes (`s3a`, `s3n` -> `s3`; `abfss` -> `abfs`), strips a single trailing
   * slash. Returns the empty string when the input is null/empty.
   */
  private[uccatalog] def normalizeLocation(raw: String): String = {
    if (raw == null || raw.isEmpty) return ""
    val schemeIdx = raw.indexOf("://")
    val (scheme, rest) = if (schemeIdx > 0) {
      (raw.substring(0, schemeIdx).toLowerCase(java.util.Locale.ROOT),
        raw.substring(schemeIdx + "://".length))
    } else {
      ("", raw)
    }
    val canonicalScheme = scheme match {
      case "s3a" | "s3n" => "s3"
      case "abfss" => "abfs"
      case other => other
    }
    val trimmedRest = if (rest.endsWith("/")) rest.dropRight(1) else rest
    if (canonicalScheme.isEmpty) trimmedRest
    else canonicalScheme + "://" + trimmedRest
  }

  /**
   * Converts a `StorageCredential.config` map into Delta-side property entries prefixed with
   * `io.unitycatalog.drc.cred.`. A credential with null/empty config emits nothing.
   *
   * <b>Security note:</b> these properties carry raw cloud credentials. Downstream consumers
   * are responsible for redacting them from log output, `EXPLAIN` output, and event logs. The
   * `io.unitycatalog.drc.cred.` prefix is a grep anchor for a future Spark-side redaction list.
   */
  private def credentialToStorageProps(
      cred: StorageCredential): Map[String, String] = {
    Option(cred.getConfig)
      .map(_.asScala.map { case (k, v) => (PROP_DRC_CREDENTIAL_PREFIX + k) -> v }.toMap)
      .getOrElse(Map.empty[String, String])
  }
}
