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

package io.delta.sharing.spark

import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.DeltaGeoSpatial
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.spark.internal.v2.read.DeltaV2ScanUtils
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager
import io.delta.sharing.client.{DeltaSharingClient, DeltaSharingRestClient}
import io.delta.sharing.client.model.{DeltaTableMetadata, Table => DeltaSharingTable}
import io.delta.kernel.{Snapshot => KernelSnapshot}
import io.delta.kernel.engine.Engine

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils}
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.read.{Scan, Statistics, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Per-table state built once when a shared table is loaded. Held by DeltaSharingV2Table and
 * threaded into the ScanBuilder/Scan. Top-level (not nested) so the Java connector classes can
 * name it.
 *
 * Holds the sharing client + metadata directly (no DeltaSharingFileIndex): the DSV2 read builds
 * its own Delta Kernel snapshot from the synthetic log.
 *
 * `readOptions` carries the time-travel pin (versionAsOf / timestampAsOf); empty for a latest read.
 * getMetadata (hence the reported schema) is fetched at the pin when the context is built, and the
 * scan reuses these same options for getFiles + the query-params hash, so both RPCs resolve the
 * same point in time.
 */
class DeltaSharingV2TableContext(
    val client: DeltaSharingClient,
    val dsTable: DeltaSharingTable,
    val dsMeta: DeltaSharingUtils.DeltaSharingTableMetadata,
    val tablePath: String,
    val readOptions: DeltaSharingOptions = new DeltaSharingOptions(Map.empty[String, String]))


/**
 * Scala bridge for the (Java) Delta Sharing DSV2 connector classes.
 */
object DeltaSharingDSV2Utils extends DeltaLogging {

  /**
   * Build the sharing client + table handle from a catalog table's storage fields (no RPC). Used by
   * [[resolveBatchSnapshotContext]]. Returns (client, table, path).
   *
   * `responseFormat` defaults to delta because the DSV2 read path is delta-format only (synthetic
   * delta log + Kernel snapshot). The read path can't consume a parquet getFiles response, so it
   * always requests delta -- unlike V1, which advertises "parquet,delta" because it has a parquet
   * fallback path that the V2 path does not. Only the routing gate
   * ([[resolveBatchSnapshotContext]]) overrides this, to ask the server which format it would
   * really serve.
   */
  private def buildSharingClient(
      spark: SparkSession,
      catalogTable: CatalogTable,
      responseFormat: String = DeltaSharingOptions.RESPONSE_FORMAT_DELTA)
      : (DeltaSharingClient, DeltaSharingTable, String) = {
    val path = CatalogUtils.URIToString(
      catalogTable.storage.locationUri.getOrElse(
        throw DeltaSharingErrors.pathNotSpecifiedException))
    val parsedPath = DeltaSharingRestClient.parsePath(path, Map.empty)
    val client = DeltaSharingRestClient(
      profileFile = parsedPath.profileFile,
      shareCredentialsOptions = Map.empty,
      forStreaming = false,
      responseFormat = responseFormat,
      // Snapshot reader features only -- the DSv2 path does not serve CDF (it falls back to V1),
      // so it must not advertise the read-time-CDF feature that getAllSupportedReaderFeatures adds.
      readerFeatures = DeltaSharingUtils.SUPPORTED_READER_FEATURES.mkString(","))
    val dsTable = DeltaSharingTable(
      share = parsedPath.share, schema = parsedPath.schema, name = parsedPath.table)
    (client, dsTable, path)
  }

  /**
   * Resolve a shared table to a DSV2 batch-snapshot read context, or None to stay on V1. Called
   * from the AbstractDeltaCatalog loadTable/loadTables guards; the returned context is handed to
   * DeltaSharingV2Table, which reuses its getMetadata (no second fetch).
   *
   * Only delta-format batch snapshots are eligible; parquet / non-delta stay on V1. `versionAsOf` /
   * `timestampAsOf` (both None for a latest read) pin the context, so getMetadata resolves at that
   * point in a single RPC; the raw strings are normalized through [[DeltaSharingOptions]] (the same
   * V1-parity validation: version+timestamp conflict, version parse, ISO-8601 timestamp). A latest
   * resolve is best-effort (any error returns None -> V1); a pinned resolve is not (see the catch).
   *
   * Format/CDF/streaming are query-time concerns invisible here, so they fall back to V1 downstream
   * (DeltaAnalysis / TableChanges.toReadQuery / DeltaSharingV2Table's V2TableWithV1Fallback).
   *
   * A read inside a multi-statement transaction (MST) also stays on V1.
   */
  def resolveBatchSnapshotContext(
      spark: SparkSession,
      catalogTable: CatalogTable,
      versionAsOf: Option[String] = None,
      timestampAsOf: Option[String] = None): Option[DeltaSharingV2TableContext] = {
    val conf = spark.sessionState.conf
    val tableName = catalogTable.identifier.unquotedString
    val forceDeltaFormat = conf.getConf(DeltaSQLConf.DELTA_SHARING_FORCE_DELTA_FORMAT)
    val isTimeTravel = versionAsOf.isDefined || timestampAsOf.isDefined
    try {
      val pin = new DeltaSharingOptions(
        versionAsOf.map(DeltaSharingOptions.TIME_TRAVEL_VERSION -> _).toMap ++
          timestampAsOf.map(DeltaSharingOptions.TIME_TRAVEL_TIMESTAMP -> _).toMap)
      // The delta read client (responseFormat=delta), held by the returned context for the later
      // getFiles. No RPC to build it.
      val (readClient, dsTable, path) = buildSharingClient(spark, catalogTable)
      if (!conf.getConf(DeltaSQLConf.DELTA_SHARING_ENABLE_DELTA_FORMAT_BATCH)) {
        logInfo(s"DSV2-Sharing: $tableName stays on V1 (delta-format batch sharing disabled)")
        None
      } else if (forceDeltaFormat) {
        // DELTA_SHARING_FORCE_DELTA_FORMAT forces every shared table onto the delta path: query
        // getMetadata with the delta read client and use it directly -- no probe, delta by
        // construction.
        val deltaTableMetadata = DeltaSharingUtils.queryDeltaTableMetadata(
          client = readClient,
          table = dsTable,
          versionAsOf = pin.versionAsOf,
          timestampAsOf = pin.timestampAsOf)
        buildContext(
          readClient, dsTable, path, deltaTableMetadata, pin, forceDeltaFormat, isTimeTravel)
      } else {
        // Auto-resolve: probe getMetadata with a "parquet,delta" client so the server's
        // respondedFormat is trustworthy. The probe must NOT use a delta-only client: that makes
        // the server always respond delta, which would wrongly route parquet-format tables to V2.
        val (probeClient, _, _) = buildSharingClient(
          spark,
          catalogTable,
          responseFormat = s"${DeltaSharingOptions.RESPONSE_FORMAT_PARQUET}," +
            s"${DeltaSharingOptions.RESPONSE_FORMAT_DELTA}")
        val deltaTableMetadata = DeltaSharingUtils.queryDeltaTableMetadata(
          client = probeClient,
          table = dsTable,
          versionAsOf = pin.versionAsOf,
          timestampAsOf = pin.timestampAsOf)
        if (deltaTableMetadata.respondedFormat == DeltaSharingOptions.RESPONSE_FORMAT_DELTA) {
          // Delta format: reuse the probe's metadata, but seed the context with the delta read
          // client so getFiles requests delta.
          buildContext(
            readClient, dsTable, path, deltaTableMetadata, pin, forceDeltaFormat, isTimeTravel)
        } else {
          logInfo(s"DSV2-Sharing: $tableName stays on V1 " +
            s"(auto-resolved respondedFormat=${deltaTableMetadata.respondedFormat})")
          None
        }
      }
    } catch {
      // Best-effort only for a latest (routing) resolve: any error keeps the table on V1. A pinned
      // (time-travel) resolve is NOT best-effort -- the table is a known delta share, so a bad
      // version / out-of-range timestamp must surface, not silently fall back and read latest.
      case NonFatal(e) if versionAsOf.isEmpty && timestampAsOf.isEmpty =>
        logWarning(s"DSV2-Sharing: $tableName stays on V1 (error resolving format: $e)")
        None
    }
  }

  /**
   * Assemble a [[DeltaSharingV2TableContext]] from an already-fetched getMetadata response
   * (no RPC), for [[resolveBatchSnapshotContext]]. `client` is the delta read client used for the
   * later getFiles. `readOptions` carries the (already-normalized) time-travel pin, empty for a
   * latest read.
   *
   * None (-> stays on V1) for a geo schema: the Kernel read path cannot convert Geometry/Geography.
   */
  private def buildContext(
      client: DeltaSharingClient,
      dsTable: DeltaSharingTable,
      path: String,
      deltaTableMetadata: DeltaTableMetadata,
      readOptions: DeltaSharingOptions,
      forceDeltaFormat: Boolean,
      isTimeTravel: Boolean): Option[DeltaSharingV2TableContext] = {
    val dsMeta = DeltaSharingUtils.getDeltaSharingTableMetadata(
      table = dsTable,
      deltaTableMetadata = deltaTableMetadata)

    if (DeltaGeoSpatial.containsGeoColumns(dsMeta.metadata.schema)) {
      logInfo(s"DSV2-Sharing: ${dsTable.name} stays on V1 (schema has a geospatial column, which " +
        s"the DSV2 Kernel read path does not support)")
      return None
    }

    logInfo(s"DSV2-Sharing: built context share=${dsTable.share} schema=${dsTable.schema} " +
      s"table=${dsTable.name} version=${dsMeta.version} " +
      s"versionAsOf=${readOptions.versionAsOf.map(_.toString).getOrElse("None")} " +
      s"timestampAsOf=${readOptions.timestampAsOf.getOrElse("None")} " +
      s"dataCols=[${dsMeta.metadata.deltaMetadata.dataSchema.fieldNames.mkString(",")}] " +
      s"partitionCols=[${dsMeta.metadata.partitionSchema.fieldNames.mkString(",")}]")


    Some(new DeltaSharingV2TableContext(
      client = client,
      dsTable = dsTable,
      dsMeta = dsMeta,
      tablePath = path,
      readOptions = readOptions))
  }

  /**
   * Curated table properties for the DSV2 surface, display-only (DESCRIBE EXTENDED / SHOW
   * TBLPROPERTIES; no read path consumes them). Shaped like DeltaV2Table.properties() /
   * DeltaTableV2: the shared table's delta.* configuration (from the already-fetched getMetadata
   * response) plus provider / location / comment.
   */
  def tableProperties(ctx: DeltaSharingV2TableContext): java.util.Map[String, String] = {
    val deltaMetadata = ctx.dsMeta.metadata.deltaMetadata
    val props = new java.util.HashMap[String, String]()
    props.putAll(deltaMetadata.configuration.asJava)
    props.put(TableCatalog.PROP_PROVIDER, "deltasharing")
    props.put(TableCatalog.PROP_LOCATION, ctx.tablePath)
    Option(deltaMetadata.description).foreach(props.put(TableCatalog.PROP_COMMENT, _))
    java.util.Collections.unmodifiableMap(props)
  }

  /**
   * The required non-partition columns: the pruned (column-projected) schema minus partition
   * columns. Passed to DeltaV2Scan as its `readDataSchema` -- the data columns Kernel actually
   * reads out of the Parquet files. Partition columns are dropped here because their values come
   * from the file's partition path (reconstructed by the reader), not from the file contents, so
   * reading them would be wrong; DeltaV2Scan re-appends them to form the full output schema.
   */
  private[spark] def prunedNonPartitionColumns(
      ctx: DeltaSharingV2TableContext, requiredSchema: StructType): StructType = {
    val partCols =
      ctx.dsMeta.metadata.partitionSchema.fieldNames.map(_.toLowerCase(Locale.ROOT)).toSet
    StructType(
      requiredSchema.fields.filterNot(f => partCols.contains(f.name.toLowerCase(Locale.ROOT))))
  }

  /**
   * The heart of the DSV2 read: issue the getFiles RPC + build the synthetic log (shared with the
   * V1 path via [[DeltaSharingDeltaLogBuilder]]), open it as a Delta Kernel snapshot, and return a
   * Delta V2 scan over it. The scan owns everything downstream:
   * file planning, statistics (post-skipping planned bytes, row counts under CBO), runtime V2
   * filtering, and the SparkBatch.
   *
   * The getFiles RPC fires here, inside ScanBuilder.build() -- the DSV2 contract orders
   * pushFilters / pushLimit / pruneColumns before build(), so `pushedFilters` and `pushedLimit` are
   * already captured by the time this runs (after pushdown, not during analysis). They become the
   * server-side hints on getFiles: the json predicate hint (built by
   * [[DeltaSharingJsonPredicates]], gated as in V1) and the limit hint.
   *
   * Both hints are best-effort -- the server may return a superset -- so the pushed filters and
   * limit are NOT re-applied by the Delta V2 scan: they stay as the residual Filter / LIMIT
   * operators Spark leaves above the scan (`pushFilters` returns every filter, `isPartiallyPushed`
   * stays true). The Delta V2 scan builder therefore does no filter pushdown of its own; it must
   * not assume any filtering happened.
   */
  def buildScan(
      spark: SparkSession,
      ctx: DeltaSharingV2TableContext,
      engine: Engine,
      requiredSchema: StructType,
      pushedFilters: Array[Filter],
      pushedLimit: java.util.OptionalLong,
      catalogStats: java.util.Optional[Statistics]): Scan = {
    DeltaSharingDataSource.setupFileSystem(spark.sqlContext)

    // 1. Convert the pushed filters to a server-side json predicate hint and unwrap the limit, then
    // do the getFiles RPC + synthetic delta log + CachedTableManager registration -> encoded path
    // (shared util). Both hints join the query-params hash, so scans of the same table with
    // different filters / limits get distinct synthetic log paths and cache entries.
    // The shared utils read versionAsOf/timestampAsOf off the DeltaSharingOptions; reuse the
    // context's readOptions (the pin fetched getMetadata at, empty for a latest read) so getFiles
    // resolves the same point.
    val jsonPredicateHints = DeltaSharingJsonPredicates.fromPushedFilters(
      pushedFilters,
      ctx.dsMeta.metadata.schema,
      ctx.dsMeta.metadata.partitionSchema.fieldNames.toSet,
      spark.sessionState.conf)
    val limit: Option[Long] = if (pushedLimit.isPresent) Some(pushedLimit.getAsLong) else None
    val queryParamsHashId = DeltaSharingUtils.getQueryParamsHashId(
      options = ctx.readOptions,
      partitionFiltersString = "",
      dataFiltersString = "",
      jsonPredicateHints = jsonPredicateHints.getOrElse(""),
      limitHint = limit.map(_.toString).getOrElse(""),
      version = ctx.dsMeta.version)
    logInfo(s"DSV2-Sharing: buildScan: getFiles RPC + building synthetic delta log " +
      s"(jsonPredicateHints=${jsonPredicateHints.isDefined}, limit=$limit)")
    // Build the synthetic log now, but defer the CachedTableManager registration until the scan's
    // snapshot manager exists, to anchor the cache entry to it (see registerPreSignedUrls below).
    val unregisteredDeltaLog = DeltaSharingDeltaLogBuilder.buildSnapshotDeltaLog(
      client = ctx.client,
      table = ctx.dsTable,
      options = ctx.readOptions,
      tablePath = ctx.tablePath,
      queryParamsHashId = queryParamsHashId,
      jsonPredicateHints = jsonPredicateHints,
      limit = limit,
      logPrefix = "DSV2-Sharing"
    )
    val encodedPath = unregisteredDeltaLog.encodedPath

    // 2. Open the synthetic log as a Delta Kernel snapshot. The caller-provided engine (a
    // DefaultEngine over the session hadoopConf, built once per table like DeltaV2Table) reads
    // through the Hadoop FileSystem, so the registered `delta-sharing-log://` FS serves the
    // fabricated _delta_log exactly as the classic DeltaLog reader does on the V1 path.
    //
    // PathBasedSnapshotManager directly, deliberately not SnapshotManagerFactory: the snapshot is
    // over the per-query synthetic log, not the shared catalog entity, so it must stay path-based
    // regardless of how the factory's dispatch evolves (a UC-managed snapshot manager would try to
    // resolve commits through UC).
    val snapshotManager = new PathBasedSnapshotManager(encodedPath.toString, engine)
    val snapshot: KernelSnapshot = snapshotManager.loadLatestSnapshot()
    if (snapshot.getVersion != 0) {
      throw new IllegalStateException(
        s"DSV2-Sharing: expected a single version-0 synthetic delta log at $encodedPath, but got " +
        s"version ${snapshot.getVersion}")
    }
    logInfo(s"DSV2-Sharing: built Kernel snapshot over $encodedPath -> DeltaV2Scan (reads via " +
      s"DeltaParquetFileFormatV2)")

    // 3. Hand the snapshot to Delta's public scan-builder factory. DeltaV2Scan itself is
    // package-private, so sharing code must depend only on Spark connector interfaces here.
    val scanBuilder = DeltaV2ScanUtils.newScanBuilder(
      ctx.dsTable.name,
      snapshot,
      engine,
      java.util.Optional.empty[CatalogTable](),
      snapshotManager,
      ctx.dsMeta.metadata.deltaMetadata.dataSchema,
      ctx.dsMeta.metadata.partitionSchema,
      ctx.dsMeta.metadata.schema,
      catalogStats,
      // Empty options: sharing options must not leak into DeltaOptions parsing.
      CaseInsensitiveStringMap.empty())
    scanBuilder.asInstanceOf[SupportsPushDownRequiredColumns].pruneColumns(requiredSchema)
    val scan = scanBuilder.build()

    // Register the presigned URLs anchored to the per-query `snapshotManager`, not `scan`:
    // DeltaV2Scan.withPrunedColumns re-plans into a new DeltaV2Scan copy, so `scan` may be
    // orphaned, but the copy shares this snapshotManager by reference.
    DeltaSharingDeltaLogBuilder.registerPreSignedUrls(
      unregisteredDeltaLog, anchor = snapshotManager)


    scan
  }

}
