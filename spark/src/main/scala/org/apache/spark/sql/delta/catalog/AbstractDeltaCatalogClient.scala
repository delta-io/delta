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

package org.apache.spark.sql.delta.catalog

import io.delta.storage.commit.uniform.UniformMetadata

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.delta.actions.{Action, DomainMetadata, Metadata, Protocol}
import org.apache.spark.sql.delta.coordinatedcommits.UCTokenBasedRestClientFactory
import org.apache.spark.sql.delta.stats.FileSizeHistogram
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Backend hook through which [[AbstractDeltaCatalog]] injects custom catalog interactions
 * that bypass the catalog operations normally provided by Spark's
 * [[org.apache.spark.sql.connector.catalog.TableCatalog]] interface (the
 * [[org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension]] delegate that
 * `AbstractDeltaCatalog` extends). Concrete implementations route table operations to a
 * catalog-specific path, e.g. talking directly to a REST endpoint instead of the
 * configured delegate, applying catalog-specific table-property handling, or vending
 * storage credentials on the returned [[Table]]. Keeping these behind a client interface
 * isolates that plumbing from `AbstractDeltaCatalog`.
 */
private[delta] trait AbstractDeltaCatalogClient {

  /**
   * Returns whether a table with `ident` exists in the catalog.
   *
   * @param ident identifier of the table to check.
   * @return `true` if the table exists in the catalog; `false` otherwise.
   */
  def tableExists(ident: Identifier): Boolean

  /**
   * Loads the table identified by `ident` from the catalog.
   *
   * @param ident identifier of the table to load.
   * @return the resolved [[Table]].
   * @throws org.apache.spark.sql.catalyst.analysis.NoSuchTableException if the catalog has
   *   no record of this identifier.
   */
  def loadTable(ident: Identifier): Table

  /**
   * Reserves a fresh staging entry with the catalog for a new Delta table and returns
   * `properties` augmented with the catalog-supplied LOCATION, the catalog-assigned table
   * id, and the storage credentials Delta needs to write the initial commit.
   *
   * This entry point is meant for fresh CREATE / CTAS only -- external, REPLACE-existing,
   * and path-based requests are filtered out upstream. Implementations should re-verify
   * that contract on entry rather than trusting the caller blindly.
   *
   * @param ident      identifier of the table being created.
   * @param properties user-supplied properties from the CREATE statement.
   * @return augmented properties carrying the staged LOCATION, the catalog-assigned table
   *         id, and the storage credentials Delta needs to write the initial commit.
   */
  def createStagingTable(
      ident: Identifier,
      properties: util.Map[String, String]): util.Map[String, String]

  /**
   * Loads the existing table from the catalog and returns `properties` augmented with
   * `PROP_IS_MANAGED_LOCATION=true` and the storage credentials Delta needs to write the
   * REPLACE commit at the existing location. Used for REPLACE / RTAS / CREATE OR REPLACE
   * on an existing catalog-managed Delta table.
   *
   * @param ident      identifier of the table being replaced.
   * @param properties user-supplied properties from the REPLACE / RTAS / CREATE OR REPLACE
   *                   statement.
   * @return augmented properties carrying `PROP_IS_MANAGED_LOCATION=true` and the storage
   *         credentials Delta needs to write at the existing location. `PROP_LOCATION` is
   *         intentionally not set; downstream Delta resolves it from the existing table.
   * @throws org.apache.spark.sql.catalyst.analysis.NoSuchTableException
   *   if the catalog has no record of this identifier.
   * @throws UnsupportedOperationException
   *   if the existing table is not MANAGED.
   */
  def loadTableAndBuildReplaceProps(
      ident: Identifier,
      properties: util.Map[String, String]): util.Map[String, String]

  /**
   * Registers a newly-written Delta table with the catalog, taking the place of the legacy
   * `super.createTable` call once Delta has produced the initial commit.
   *
   * @param ident                 identifier of the table to register.
   * @param table                 Spark V1 [[CatalogTable]] describing the table (identifier,
   *                              storage, schema, partitioning, properties, comment).
   * @param metadata              Delta [[Metadata]] action produced by the initial commit
   *                              (schema, partition columns, configuration).
   * @param domainMetadata        Delta [[DomainMetadata]] actions produced by the initial
   *                              commit, if any.
   * @param protocol              Delta [[Protocol]] action produced by the initial commit
   *                              (reader / writer versions and table features).
   * @param lastCommitTimestampMs wall-clock timestamp of the latest commit that produced
   *                              `metadata` / `protocol`, used by the catalog as the
   *                              authoritative "last updated" timestamp on the registered
   *                              entry.
   * @param uniformMetadata       UniForm Iceberg metadata generated atomically with the initial
   *                              snapshot;
   *                              [[None]] when the table was not created with UniForm enabled.
   */
  def createTable(
      ident: Identifier,
      table: CatalogTable,
      metadata: Metadata,
      domainMetadata: Seq[DomainMetadata],
      protocol: Protocol,
      lastCommitTimestampMs: Long,
      uniformMetadata: Option[UniformMetadata] = None): Unit

  /**
   * Reports post-commit telemetry for the table to the catalog. Implementations build
   * whatever payload their catalog expects from the per-commit fields and the
   * `CatalogTable`'s storage properties (e.g. the catalog-side table id), then ship it.
   *
   * Exceptions bubble out; implementations must not swallow failures internally.
   *
   * @param ct                catalog metadata for the committed table.
   * @param committedActions  actions written in this commit (used to derive file/row counts).
   * @param committedVersion  the commit version (used as the histogram's commit version).
   * @param snapshotHistogram post-commit file-size distribution read from the CRC; `None`
   *                          when the CRC is unavailable. Read from the CRC only to avoid
   *                          triggering state reconstruction.
   */
  def reportMetrics(
      ct: CatalogTable,
      committedActions: Seq[Action],
      committedVersion: Long,
      snapshotHistogram: Option[FileSizeHistogram]): Unit
}

/** Builds a [[AbstractDeltaCatalogClient]] from catalog options. */
private[catalog] trait AbstractDeltaCatalogClientFactory {
  def fromCatalogOptions(
      catalogName: String,
      options: util.Map[String, String],
      fallbackLoadTableFunc: Identifier => Table): AbstractDeltaCatalogClient
}

/**
 * Factory entry point for [[AbstractDeltaCatalogClient]] instances. Used both by
 * `AbstractDeltaCatalog.initialize` (to wire the catalog's own client at construction
 * time) and by the post-commit metrics hook (to build a fresh per-commit client without
 * caching state across commits).
 */
private[delta] object AbstractDeltaCatalogClient extends Logging {

  private val UC_DELTA_CATALOG_CLIENT_IMPL_CLASS_NAME: String =
    "org.apache.spark.sql.delta.catalog.UCDeltaCatalogClientImpl"

  /**
   * Marks a carried-forward `delta.*` property (from a catalog-managed REPLACE) that this Delta
   * version doesn't recognize, so it can bypass validation and survive into the replaced table.
   * Intentionally neither `delta.`- nor `option.`-prefixed so it rides untouched through the
   * property plumbing until `AbstractDeltaCatalog.createDeltaTable` strips and re-injects it.
   */
  private[delta] val CARRY_FORWARD_PREFIX: String = "__replaceCarryForward."

  /**
   * Returns a [[AbstractDeltaCatalogClient]] wrapped in [[Some]] unless the catalog has
   * opted out via `deltaRestApi.enabled=false`, in which case returns [[None]]. The flag
   * defaults to `true` when absent, so any UC catalog goes through the UC Delta API path
   * unless the operator explicitly disables it. The concrete impl is loaded reflectively so
   * [[AbstractDeltaCatalog]] doesn't compile-depend on it; if loading fails, throws
   * [[IllegalStateException]] rather than silently degrading.
   */
  def fromCatalogOptionsIfEnabled(
      catalogName: String,
      options: CaseInsensitiveStringMap,
      fallbackLoadTableFunc: Identifier => Table): Option[AbstractDeltaCatalogClient] = {
    val optionsMap = new util.HashMap[String, String](options.asCaseSensitiveMap())
    val key = UCTokenBasedRestClientFactory.DELTA_REST_API_ENABLED_KEY
    if (!optionsMap.getOrDefault(key, "true").toBoolean) {
      return None
    }
    val factory = try {
      // scalastyle:off classforname
      val cls = Class.forName(UC_DELTA_CATALOG_CLIENT_IMPL_CLASS_NAME + "$")
      // scalastyle:on classforname
      cls.getField("MODULE$").get(null).asInstanceOf[AbstractDeltaCatalogClientFactory]
    } catch {
      case e: Exception =>
        throw new IllegalStateException(
          s"Failed to load $UC_DELTA_CATALOG_CLIENT_IMPL_CLASS_NAME though '$key' is true. " +
            "Ensure the implementation JAR is on the classpath, or remove " +
            s"'$key' from the catalog options to fall back to the legacy delegate.", e)
    }
    Some(factory.fromCatalogOptions(catalogName, optionsMap, fallbackLoadTableFunc))
  }
}
