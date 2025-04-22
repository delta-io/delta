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

package org.apache.spark.sql.delta

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.catalyst.TimeTravel
import org.apache.spark.sql.delta.DataFrameUtils
import org.apache.spark.sql.delta.DeltaErrors.{TemporallyUnstableInputException, TimestampEarlierThanCommitRetentionException}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.catalog.IcebergTablePlaceHolder
import org.apache.spark.sql.delta.commands._
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.constraints.{AddConstraint, DropConstraint}
import org.apache.spark.sql.delta.coordinatedcommits.{CatalogOwnedTableUtils, CoordinatedCommitsUtils}
import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources._
import org.apache.spark.sql.delta.util.AnalysisHelper
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.CloneTableStatement
import org.apache.spark.sql.catalyst.plans.logical.RestoreTableStatement
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.WriteToStream
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttribute
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.CreateTableLikeCommand
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, LogicalRelationShims, LogicalRelationWithTable}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap


/**
 * Analysis rules for Delta. Currently, these rules enable schema enforcement / evolution with
 * INSERT INTO.
 */
class DeltaAnalysis(session: SparkSession)
  extends Rule[LogicalPlan] with AnalysisHelper with DeltaLogging {

  type CastFunction = (Expression, DataType, String) => Expression

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    // INSERT INTO by ordinal and df.insertInto()
    case a @ AppendDelta(r, d) if !a.isByName &&
        needsSchemaAdjustmentByOrdinal(d, a.query, r.schema, a.writeOptions) =>
      val projection = resolveQueryColumnsByOrdinal(a.query, r.output, d, a.writeOptions)
      if (projection != a.query) {
        a.copy(query = projection)
      } else {
        a
      }


    // INSERT INTO by name
    // AppendData.byName is also used for DataFrame append so we check for the SQL origin text
    // since we only want to up-cast for SQL insert into by name
    case a @ AppendDelta(r, d) if a.isByName && a.origin.sqlText.nonEmpty &&
        needsSchemaAdjustmentByName(a.query, r.output, d, a.writeOptions) =>
      val projection = resolveQueryColumnsByName(a.query, r.output, d, a.writeOptions)
      if (projection != a.query) {
        a.copy(query = projection)
      } else {
        a
      }

    /**
     * Handling create table like when a delta target (provider)
     * is provided explicitly or when the source table is a delta table
     */
    case EligibleCreateTableLikeCommand(ctl, src) =>
      val deltaTableIdentifier = DeltaTableIdentifier(session, ctl.targetTable)

      // Check if table is given by path
      val isTableByPath = DeltaTableIdentifier.isDeltaPath(session, ctl.targetTable)

      // Check if targetTable is given by path
      val targetTableIdentifier =
        if (isTableByPath) {
          TableIdentifier(deltaTableIdentifier.toString)
        } else {
          ctl.targetTable
        }

      val newStorage =
        if (ctl.fileFormat.inputFormat.isDefined) {
          ctl.fileFormat
        } else if (isTableByPath) {
          src.storage.copy(locationUri =
            Some(deltaTableIdentifier.get.getPath(session).toUri))
        } else {
          src.storage.copy(locationUri = ctl.fileFormat.locationUri)
        }

      // If the location is specified or target table is given
      // by path, we create an external table.
      // Otherwise create a managed table.
      val tblType =
        if (newStorage.locationUri.isEmpty && !isTableByPath) {
          CatalogTableType.MANAGED
        } else {
          CatalogTableType.EXTERNAL
        }


      // Whether we are enabling Catalog-Owned via explicit property overrides.
      var isEnablingCatalogOwnedViaExplicitPropertyOverrides: Boolean = false

      val catalogTableTarget =
        // If source table is Delta format
        if (src.provider.exists(DeltaSourceUtils.isDeltaDataSourceName)) {
          val deltaLogSrc = DeltaTableV2(session, new Path(src.location))

          // Column mapping and row tracking fields cannot be set externally. If the features are
          // used on the source delta table, then the corresponding fields would be set for the
          // sourceTable and needs to be removed from the targetTable's configuration. The fields
          // will then be set in the targetTable's configuration internally after.
          //
          // Coordinated commits/Catalog-Owned configurations from the source delta table should
          // also be left out, since CREATE LIKE is similar to CLONE, and we do not copy the
          // commit coordinator from the source table.
          // If users want a commit coordinator for the target table, they can
          // specify the configurations in the CREATE LIKE command explicitly.
          val sourceMetadata = deltaLogSrc.initialSnapshot.metadata

          // Catalog-Owned: Specifying the table UUID in the TBLPROPERTIES clause
          // should be blocked.
          CatalogOwnedTableUtils.validateUCTableIdNotPresent(property = ctl.properties)

          // Check whether we are trying to enable Catalog-Owned via explicit property overrides.
          // The reason to check this is, if the source table is a Catalog-Owned table, and
          // we are also trying to enable Catalog-Owned for the target table - We do *NOT*
          // want to filter out [[CatalogOwnedTableFeature]] from the source table. If we do that,
          // the resulting target table's protocol will *NOT* have CatalogOwned table feature
          // present though we have explicitly specified it in the TBLPROPERTIES clause.
          // This only applies to cases where source table has Catalog-Owned enabled.
          // It works as intended if source table is a normal delta table.
          if (TableFeatureProtocolUtils.getSupportedFeaturesFromTableConfigs(
                configs = ctl.properties).contains(CatalogOwnedTableFeature)) {
            isEnablingCatalogOwnedViaExplicitPropertyOverrides = true
          }

          val config =
            sourceMetadata.configuration.-("delta.columnMapping.maxColumnId")
              .-(MaterializedRowId.MATERIALIZED_COLUMN_NAME_PROP)
              .-(MaterializedRowCommitVersion.MATERIALIZED_COLUMN_NAME_PROP)
              .filterKeys(!CoordinatedCommitsUtils.TABLE_PROPERTY_KEYS.contains(_)).toMap
              // Catalog-Owned: Do not copy table UUID from source table
              .filterKeys(_ != UCCommitCoordinatorClient.UC_TABLE_ID_KEY).toMap

          new CatalogTable(
            identifier = targetTableIdentifier,
            tableType = tblType,
            storage = newStorage,
            schema = sourceMetadata.schema,
            properties = config ++ ctl.properties,
            partitionColumnNames = sourceMetadata.partitionColumns,
            provider = Some("delta"),
            comment = Option(sourceMetadata.description)
          )
        } else { // Source table is not delta format
            new CatalogTable(
              identifier = targetTableIdentifier,
              tableType = tblType,
              storage = newStorage,
              schema = src.schema,
              properties = src.properties ++ ctl.properties,
              partitionColumnNames = src.partitionColumnNames,
              provider = Some("delta"),
              comment = src.comment
            )
        }
      val saveMode =
        if (ctl.ifNotExists) {
          SaveMode.Ignore
        } else {
          SaveMode.ErrorIfExists
        }

      val protocol =
        if (src.provider.exists(DeltaSourceUtils.isDeltaDataSourceName)) {
          Some(DeltaTableV2(session, new Path(src.location)).initialSnapshot.protocol)
        } else {
          None
        }
      // Catalog-Owned: Do not copy over [[CatalogOwnedTableFeature]] from source table
      //                except the certain case.
      val protocolAfterFilteringCatalogOwnedFromSource = protocol match {
        case Some(p) if !isEnablingCatalogOwnedViaExplicitPropertyOverrides =>
          // Only filter out [[CatalogOwnedTableFeature]] when target table is not enabling
          // CatalogOwned.
          // E.g.,
          // - CREATE TABLE t1 LIKE t2
          //   - Filter CatalogOwned table feature out since target table is not enabling
          //     CatalogOwned explicitly.
          // - CREATE TABLE t1 LIKE t2 TBLPROPERTIES (
          //     'delta.feature.catalogOwned-preview' = 'supported'
          //   )
          //   - Do not filter CatalogOwned table feature out if target table is enabling
          //     CatalogOwned.
          Some(CatalogOwnedTableUtils.filterOutCatalogOwnedTableFeature(protocol = p))
        case _ =>
          protocol
      }
      val newDeltaCatalog = new DeltaCatalog()
      val existingTableOpt = newDeltaCatalog.getExistingTableIfExists(catalogTableTarget.identifier)
      val newTable = newDeltaCatalog
        .verifyTableAndSolidify(
          catalogTableTarget,
          None
        )
      CreateDeltaTableCommand(
        table = newTable,
        existingTableOpt = existingTableOpt,
        mode = saveMode,
        query = None,
        output = ctl.output,
        protocol = protocolAfterFilteringCatalogOwnedFromSource,
        tableByPath = isTableByPath)

    // INSERT OVERWRITE by ordinal and df.insertInto()
    case o @ OverwriteDelta(r, d) if !o.isByName &&
        needsSchemaAdjustmentByOrdinal(d, o.query, r.schema, o.writeOptions) =>
      val projection = resolveQueryColumnsByOrdinal(o.query, r.output, d, o.writeOptions)
      if (projection != o.query) {
        val aliases = AttributeMap(o.query.output.zip(projection.output).collect {
          case (l: AttributeReference, r: AttributeReference) if !l.sameRef(r) => (l, r)
        })
        val newDeleteExpr = o.deleteExpr.transformUp {
          case a: AttributeReference => aliases.getOrElse(a, a)
        }
        o.copy(deleteExpr = newDeleteExpr, query = projection)
      } else {
        o
      }

    // INSERT OVERWRITE by name
    // OverwriteDelta.byName is also used for DataFrame append so we check for the SQL origin text
    // since we only want to up-cast for SQL insert into by name
    case o @ OverwriteDelta(r, d) if o.isByName && o.origin.sqlText.nonEmpty &&
        needsSchemaAdjustmentByName(o.query, r.output, d, o.writeOptions) =>
      val projection = resolveQueryColumnsByName(o.query, r.output, d, o.writeOptions)
      if (projection != o.query) {
        val aliases = AttributeMap(o.query.output.zip(projection.output).collect {
          case (l: AttributeReference, r: AttributeReference) if !l.sameRef(r) => (l, r)
        })
        val newDeleteExpr = o.deleteExpr.transformUp {
          case a: AttributeReference => aliases.getOrElse(a, a)
        }
        o.copy(deleteExpr = newDeleteExpr, query = projection)
      } else {
        o
      }


    // INSERT OVERWRITE with dynamic partition overwrite
    case o @ DynamicPartitionOverwriteDelta(r, d) if o.resolved
      =>
      val adjustedQuery = if (!o.isByName &&
          needsSchemaAdjustmentByOrdinal(d, o.query, r.schema, o.writeOptions)) {
        // INSERT OVERWRITE by ordinal and df.insertInto()
        resolveQueryColumnsByOrdinal(o.query, r.output, d, o.writeOptions)
      } else if (o.isByName && o.origin.sqlText.nonEmpty &&
          needsSchemaAdjustmentByName(o.query, r.output, d, o.writeOptions)) {
        // INSERT OVERWRITE by name
        // OverwriteDelta.byName is also used for DataFrame append so we check for the SQL origin
        // text since we only want to up-cast for SQL insert into by name
        resolveQueryColumnsByName(o.query, r.output, d, o.writeOptions)
      } else {
        o.query
      }
      DeltaDynamicPartitionOverwriteCommand(r, d, adjustedQuery, o.writeOptions, o.isByName)

    case ResolveDeltaTableWithPartitionFilters(plan) => plan

    // SQL CDC table value functions "table_changes" and "table_changes_by_path"
    case stmt: CDCStatementBase if stmt.functionArgs.forall(_.resolved) =>
      stmt.toTableChanges(session)

    case tc: TableChanges if tc.child.resolved => tc.toReadQuery


    // Here we take advantage of CreateDeltaTableCommand which takes a LogicalPlan for CTAS in order
    // to perform CLONE. We do this by passing the CloneTableCommand as the query in
    // CreateDeltaTableCommand and let Create handle the creation + checks of creating a table in
    // the metastore instead of duplicating that effort in CloneTableCommand.
    case cloneStatement: CloneTableStatement =>
      // Get the info necessary to CreateDeltaTableCommand
      EliminateSubqueryAliases(cloneStatement.source) match {
        case DataSourceV2Relation(table: DeltaTableV2, _, _, _, _) =>
          resolveCloneCommand(cloneStatement.target, new CloneDeltaSource(table), cloneStatement)

        // Pass the traveled table if a previous version is to be cloned
        case tt @ TimeTravel(DataSourceV2Relation(tbl: DeltaTableV2, _, _, _, _), _, _, _)
            if tt.expressions.forall(_.resolved) =>
          val ttSpec = DeltaTimeTravelSpec(tt.timestamp, tt.version, tt.creationSource)
          val traveledTable = tbl.copy(timeTravelOpt = Some(ttSpec))
          resolveCloneCommand(
            cloneStatement.target, new CloneDeltaSource(traveledTable), cloneStatement)

        case DataSourceV2Relation(table: IcebergTablePlaceHolder, _, _, _, _) =>
          resolveCloneCommand(
            cloneStatement.target,
            CloneIcebergSource(
              table.tableIdentifier, sparkTable = None, deltaSnapshot = None, session),
            cloneStatement)

        case DataSourceV2Relation(table, _, _, _, _)
            if table.getClass.getName.endsWith("org.apache.iceberg.spark.source.SparkTable") =>
          val tableIdent = Try {
            CatalystSqlParser.parseTableIdentifier(table.name())
          } match {
            case Success(ident) => ident
            case Failure(_: ParseException) =>
              // Fallback to 2-level identifier to make compatible with older Apache spark,
              // this ident will NOT be used to look up the Iceberg tables later.
              CatalystSqlParser.parseMultipartIdentifier(table.name()).tail.asTableIdentifier
            case Failure(e) => throw e
          }
          resolveCloneCommand(
            cloneStatement.target,
            CloneIcebergSource(tableIdent, Some(table), deltaSnapshot = None, session),
            cloneStatement)

        case u: UnresolvedRelation =>
          u.tableNotFound(u.multipartIdentifier)

        case TimeTravel(u: UnresolvedRelation, _, _, _) =>
          u.tableNotFound(u.multipartIdentifier)

        case LogicalRelationWithTable(
            HadoopFsRelation(location, _, _, _, _: ParquetFileFormat, _), catalogTable) =>
          val tableIdent = catalogTable.map(_.identifier)
            .getOrElse(TableIdentifier(location.rootPaths.head.toString, Some("parquet")))
          val provider = if (catalogTable.isDefined) {
            catalogTable.get.provider.getOrElse("Unknown")
          } else {
            "parquet"
          }
          // Only plain Parquet sources are eligible for CLONE, extensions like 'deltaSharing' are
          // NOT supported.
          if (!provider.equalsIgnoreCase("parquet")) {
            throw DeltaErrors.cloneFromUnsupportedSource(
              tableIdent.unquotedString,
              provider)
          }

          resolveCloneCommand(
            cloneStatement.target,
            CloneParquetSource(tableIdent, catalogTable, session), cloneStatement)

        case HiveTableRelation(catalogTable, _, _, _, _) =>
          if (!ConvertToDeltaCommand.isHiveStyleParquetTable(catalogTable)) {
            throw DeltaErrors.cloneFromUnsupportedSource(
              catalogTable.identifier.unquotedString,
              catalogTable.storage.serde.getOrElse("Unknown"))
          }
          resolveCloneCommand(
            cloneStatement.target,
            CloneParquetSource(catalogTable.identifier, Some(catalogTable), session),
            cloneStatement)

        case v: View =>
          throw DeltaErrors.cloneFromUnsupportedSource(
            v.desc.identifier.unquotedString, "View")

        case l: LogicalPlan =>
          throw DeltaErrors.cloneFromUnsupportedSource(
            l.toString, "Unknown")
      }

    case restoreStatement @ RestoreTableStatement(target) =>
      EliminateSubqueryAliases(target) match {
        // Pass the traveled table if a previous version is to be cloned
        case tt @ TimeTravel(DataSourceV2Relation(tbl: DeltaTableV2, _, _, _, _), _, _, _)
            if tt.expressions.forall(_.resolved) =>
          val ttSpec = DeltaTimeTravelSpec(tt.timestamp, tt.version, tt.creationSource)
          val traveledTable = tbl.copy(timeTravelOpt = Some(ttSpec))
          // restoring to same version as latest should be a no-op.
          val sourceSnapshot = try {
            traveledTable.initialSnapshot
          } catch {
            case v: VersionNotFoundException =>
              throw DeltaErrors.restoreVersionNotExistException(v.userVersion, v.earliest, v.latest)
            case tEarlier: TimestampEarlierThanCommitRetentionException =>
              throw DeltaErrors.restoreTimestampBeforeEarliestException(
                tEarlier.userTimestamp.toString,
                tEarlier.commitTs.toString
              )
            case tUnstable: TemporallyUnstableInputException =>
              throw DeltaErrors.restoreTimestampGreaterThanLatestException(
                tUnstable.userTimestamp.toString,
                tUnstable.commitTs.toString
              )
          }
          // TODO: Fetch the table version from deltaLog.update().version to guarantee freshness.
          //  This can also be used by RestoreTableCommand
          if (sourceSnapshot.version == traveledTable.deltaLog.unsafeVolatileSnapshot.version) {
            return LocalRelation(restoreStatement.output)
          }

          RestoreTableCommand(traveledTable)

        case u: UnresolvedRelation =>
          u.tableNotFound(u.multipartIdentifier)

        case TimeTravel(u: UnresolvedRelation, _, _, _) =>
          u.tableNotFound(u.multipartIdentifier)

        case _ =>
          throw DeltaErrors.notADeltaTableException("RESTORE")
      }

    // Resolve as a resolved table if the path is for delta table. For non delta table, we keep the
    // path and pass it along in a ResolvedPathBasedNonDeltaTable. This is needed as DESCRIBE DETAIL
    // supports both delta and non delta paths.
    case u: UnresolvedPathBasedTable =>
      val table = getPathBasedDeltaTable(u.path, u.options)
      if (Try(table.tableExists).getOrElse(false)) {
        // Resolve it as a path-based Delta table
        val catalog = session.sessionState.catalogManager.currentCatalog.asTableCatalog
        ResolvedTable.create(
          catalog, Identifier.of(Array(DeltaSourceUtils.ALT_NAME), u.path), table)
      } else {
        // Resolve it as a placeholder, to identify it as a non-Delta table.
        ResolvedPathBasedNonDeltaTable(u.path, u.options, u.commandName)
      }

    case u: UnresolvedPathBasedDeltaTable =>
      val table = getPathBasedDeltaTable(u.path, u.options)
      if (!table.tableExists) {
        throw DeltaErrors.notADeltaTableException(u.commandName, u.deltaTableIdentifier)
      }
      val catalog = session.sessionState.catalogManager.currentCatalog.asTableCatalog
      ResolvedTable.create(catalog, u.identifier, table)

    case u: UnresolvedPathBasedDeltaTableRelation =>
      val table = getPathBasedDeltaTable(u.path, u.options.asScala.toMap)
      if (!table.tableExists) {
        throw DeltaErrors.notADeltaTableException(u.deltaTableIdentifier)
      }
      DataSourceV2Relation.create(table, None, Some(u.identifier), u.options)

    case d: DescribeDeltaHistory if d.childrenResolved => d.toCommand

    case FallbackToV1DeltaRelation(v1Relation) => v1Relation

    case ResolvedTable(_, _, d: DeltaTableV2, _) if d.catalogTable.isEmpty && !d.tableExists =>
      // This is DDL on a path based table that doesn't exist. CREATE will not hit this path, most
      // SHOW / DESC code paths will hit this
      throw DeltaErrors.notADeltaTableException(DeltaTableIdentifier(path = Some(d.path.toString)))

    // DML - TODO: Remove these Delta-specific DML logical plans and use Spark's plans directly

    case d @ DeleteFromTable(table, condition) if d.childrenResolved =>
      // rewrites Delta from V2 to V1
      val newTarget = stripTempViewWrapper(table).transformUp { case DeltaRelation(lr) => lr }
      val indices = newTarget.collect {
        case DeltaFullTable(_, index) => index
      }
      if (indices.isEmpty) {
        // Not a Delta table at all, do not transform
        d
      } else if (indices.size == 1 && indices(0).deltaLog.tableExists) {
        // It is a well-defined Delta table with a schema
        DeltaDelete(newTarget, Some(condition))
      } else {
        // Not a well-defined Delta table
        throw DeltaErrors.notADeltaSourceException("DELETE", Some(d))
      }

    case u @ UpdateTable(table, assignments, condition) if u.childrenResolved =>
      val (cols, expressions) = assignments.map(a => a.key -> a.value).unzip
      // rewrites Delta from V2 to V1
      val newTable = stripTempViewWrapper(table).transformUp { case DeltaRelation(lr) => lr }
        newTable.collectLeaves().headOption match {
          case Some(DeltaFullTable(_, index)) =>
            DeltaUpdateTable(newTable, cols, expressions, condition)
          case o =>
            // not a Delta table
            u
        }


    case merge: MergeIntoTable if merge.childrenResolved =>
      val matchedActions = merge.matchedActions.map {
        case update: UpdateAction =>
          DeltaMergeIntoMatchedUpdateClause(
            update.condition,
            DeltaMergeIntoClause.toActions(update.assignments))
        case update: UpdateStarAction =>
          DeltaMergeIntoMatchedUpdateClause(update.condition, DeltaMergeIntoClause.toActions(Nil))
        case delete: DeleteAction =>
          DeltaMergeIntoMatchedDeleteClause(delete.condition)
        case other =>
          throw new IllegalArgumentException(
            s"${other.prettyName} clauses cannot be part of the WHEN MATCHED clause in MERGE INTO.")
      }
      val notMatchedActions = merge.notMatchedActions.map {
        case insert: InsertAction =>
          DeltaMergeIntoNotMatchedInsertClause(
            insert.condition,
            DeltaMergeIntoClause.toActions(insert.assignments))
        case insert: InsertStarAction =>
          DeltaMergeIntoNotMatchedInsertClause(
            insert.condition, DeltaMergeIntoClause.toActions(Nil))
        case other =>
          throw new IllegalArgumentException(
            s"${other.prettyName} clauses cannot be part of the WHEN NOT MATCHED clause in MERGE " +
             "INTO.")
      }
      val notMatchedBySourceActions = merge.notMatchedBySourceActions.map {
        case update: UpdateAction =>
          DeltaMergeIntoNotMatchedBySourceUpdateClause(
            update.condition,
            DeltaMergeIntoClause.toActions(update.assignments))
        case delete: DeleteAction =>
          DeltaMergeIntoNotMatchedBySourceDeleteClause(delete.condition)
        case other =>
          throw new IllegalArgumentException(
            s"${other.prettyName} clauses cannot be part of the WHEN NOT MATCHED BY SOURCE " +
             "clause in MERGE INTO.")
      }
      // rewrites Delta from V2 to V1
      var isDelta = false
      val newTarget = stripTempViewForMergeWrapper(merge.targetTable).transformUp {
        case DeltaRelation(lr) =>
          isDelta = true
          lr
      }

      if (isDelta) {
        // Even if we're merging into a non-Delta target, we will catch it later and throw an
        // exception.
        val deltaMerge = DeltaMergeInto(
          newTarget,
          merge.sourceTable,
          merge.mergeCondition,
          matchedActions ++ notMatchedActions ++ notMatchedBySourceActions,
          // TODO: We are waiting for Spark to support the SQL "WITH SCHEMA EVOLUTION" syntax.
          // After that this argument will be `merge.withSchemaEvolution`.
          withSchemaEvolution = false
        )

        ResolveDeltaMergeInto.resolveReferencesAndSchema(deltaMerge, conf)(
          tryResolveReferencesForExpressions(session))
      } else {
        merge
      }

    case merge: MergeIntoTable if merge.targetTable.exists(_.isInstanceOf[DataSourceV2Relation]) =>
      // When we hit here, it means the MERGE source is not resolved and we can't convert the MERGE
      // command to the Delta variant. We need to add a special marker to the target table, so that
      // this rule does not convert it to v1 relation too early, as we need to keep it as a v2
      // relation to bypass the OSS MERGE resolution code in the rule `ResolveReferences`.
      merge.targetTable.foreach {
        // TreeNodeTag is not very reliable, but it's OK to use it here, as we will use it very
        // soon: when this rule transforms down the plan tree and hits the MERGE target table.
        // There is no chance in this rule that we will drop this tag. At the end, This rule will
        // turn MergeIntoTable into DeltaMergeInto, and convert all Delta relations inside it to
        // v1 relations (no need to clean up this tag).
        case r: DataSourceV2Relation => r.setTagValue(DeltaRelation.KEEP_AS_V2_RELATION_TAG, ())
        case _ =>
      }
      merge

    case reorg @ DeltaReorgTable(resolved @ ResolvedTable(_, _, _: DeltaTableV2, _), spec) =>
      DeltaReorgTableCommand(resolved, spec)(reorg.predicates)

    case DeltaReorgTable(ResolvedTable(_, _, t, _), _) =>
      throw DeltaErrors.notADeltaTable(t.name())

    case cmd @ ShowColumns(child @ ResolvedTable(_, _, table: DeltaTableV2, _), namespace, _) =>
      // Adapted from the rule in spark ResolveSessionCatalog.scala, which V2 tables don't trigger.
      // NOTE: It's probably a spark bug to check head instead of tail, for 3-part identifiers.
      val resolver = session.sessionState.analyzer.resolver
      val v1TableName = child.identifier.asTableIdentifier
      namespace.foreach { ns =>
        if (v1TableName.database.exists(!resolver(_, ns.head))) {
          throw DeltaThrowableHelperShims.showColumnsWithConflictDatabasesError(ns, v1TableName)
        }
      }
      ShowDeltaTableColumnsCommand(child)

    case deltaMerge: DeltaMergeInto =>
      val d = if (deltaMerge.childrenResolved && !deltaMerge.resolved) {
        ResolveDeltaMergeInto.resolveReferencesAndSchema(deltaMerge, conf)(
          tryResolveReferencesForExpressions(session))
      } else deltaMerge
      d.copy(target = stripTempViewForMergeWrapper(d.target))

    case origStreamWrite: WriteToStream =>
      // The command could have Delta as source and/or sink. We need to look at both.
      val streamWrite = origStreamWrite match {
        case WriteToStream(_, _, sink @ DeltaSink(_, _, _, _, _, None), _, _, _, _, Some(ct)) =>
          // The command has a catalog table, but the DeltaSink does not. This happens because
          // DeltaDataSource.createSink (Spark API) didn't have access to the catalog table when it
          // created the DeltaSink. Fortunately we can fix it up here.
          origStreamWrite.copy(sink = sink.copy(catalogTable = Some(ct)))
        case _ => origStreamWrite
      }

      // We also need to validate the source schema location, if the command has a Delta source.
      verifyDeltaSourceSchemaLocation(
        streamWrite.inputQuery, streamWrite.resolvedCheckpointLocation)
      streamWrite

  }

  /**
   * Creates a catalog table for CreateDeltaTableCommand.
   *
   * @param targetPath Target path containing the target path to clone to
   * @param byPath Whether the target is a path based table
   * @param tableIdent Table Identifier for the target table
   * @param targetLocation User specified target location for the new table
   * @param existingTable Existing table definition if we're going to be replacing the table
   * @param srcTable The source table to clone
   * @return catalog to CreateDeltaTableCommand with
   */
  private def createCatalogTableForCloneCommand(
      targetPath: Path,
      byPath: Boolean,
      tableIdent: TableIdentifier,
      targetLocation: Option[String],
      existingTable: Option[CatalogTable],
      srcTable: CloneSource,
      propertiesOverrides: Map[String, String]): CatalogTable = {
    // If external location is defined then then table is an external table
    // If the table is a path-based table, we also say that the table is external even if no
    // metastore table will be created. This is done because we are still explicitly providing a
    // locationUri which is behavior expected only of external tables
    // In the case of ifNotExists being true and a table existing at the target destination, create
    // a managed table so we don't have to pass a fake path
    val (tableType, storage) = if (targetLocation.isDefined || byPath) {
      (CatalogTableType.EXTERNAL,
        CatalogStorageFormat.empty.copy(locationUri = Some(targetPath.toUri)))
    } else {
      (CatalogTableType.MANAGED, CatalogStorageFormat.empty)
    }
    var properties = srcTable.metadata.configuration
    val validatedOverrides = DeltaConfigs.validateConfigurations(propertiesOverrides)
    properties = properties.filterKeys(!validatedOverrides.keySet.contains(_)).toMap ++
      validatedOverrides

    new CatalogTable(
      identifier = tableIdent,
      tableType = tableType,
      storage = storage,
      schema = srcTable.schema,
      properties = properties,
      provider = Some("delta"),
      stats = existingTable.flatMap(_.stats)
    )
  }

  private def getPathBasedDeltaTable(path: String, options: Map[String, String]): DeltaTableV2 = {
    DeltaTableV2(session, new Path(path), options = options)
  }

  private def resolveCreateTableMode(
      isCreate: Boolean,
      isReplace: Boolean,
      ifNotExist: Boolean): (SaveMode, TableCreationModes.CreationMode) = {
    val saveMode = if (isReplace) {
      SaveMode.Overwrite
    } else if (ifNotExist) {
      SaveMode.Ignore
    } else {
      SaveMode.ErrorIfExists
    }

    val tableCreationMode = if (isCreate && isReplace) {
      TableCreationModes.CreateOrReplace
    } else if (isCreate) {
      TableCreationModes.Create
    } else {
      TableCreationModes.Replace
    }

    (saveMode, tableCreationMode)
  }

  /**
   * Instantiates a CreateDeltaTableCommand with CloneTableCommand as the child query.
   *
   * @param targetPlan the target of Clone as passed in a LogicalPlan
   * @param sourceTbl the DeltaTableV2 that was resolved as the source of the clone command
   * @return Resolve the clone command as the query in a CreateDeltaTableCommand.
   */
  private def resolveCloneCommand(
      targetPlan: LogicalPlan,
      sourceTbl: CloneSource,
      statement: CloneTableStatement): LogicalPlan = {
    val isReplace = statement.isReplaceCommand
    val isCreate = statement.isCreateCommand
    val ifNotExists = statement.ifNotExists

    val analyzer = session.sessionState.analyzer
    import analyzer.{NonSessionCatalogAndIdentifier, SessionCatalogAndIdentifier}
    val targetLocation = statement.targetLocation
    val (saveMode, tableCreationMode) = resolveCreateTableMode(isCreate, isReplace, ifNotExists)
    // We don't use information in the catalog if the table is time travelled
    val sourceCatalogTable = if (sourceTbl.timeTravelOpt.isDefined) None else sourceTbl.catalogTable

    EliminateSubqueryAliases(targetPlan) match {
      // Target is a path based table
      case DataSourceV2Relation(targetTbl: DeltaTableV2, _, _, _, _) if !targetTbl.tableExists =>
        val path = targetTbl.path
        val tblIdent = TableIdentifier(path.toString, Some("delta"))
        if (!isCreate) {
          throw DeltaErrors.cannotReplaceMissingTableException(
            Identifier.of(Array("delta"), path.toString))
        }
        // Trying to clone something on itself should be a no-op
        if (sourceTbl == new CloneDeltaSource(targetTbl)) {
          return LocalRelation()
        }
        // If this is a path based table and an external location is also defined throw an error
        if (statement.targetLocation.exists(loc => new Path(loc).toString != path.toString)) {
          throw DeltaErrors.cloneAmbiguousTarget(statement.targetLocation.get, tblIdent)
        }
        // We're creating a table by path and there won't be a place to store catalog stats
        val catalog = createCatalogTableForCloneCommand(path, byPath = true, tblIdent,
          targetLocation, sourceCatalogTable, sourceTbl, statement.tablePropertyOverrides)
        CreateDeltaTableCommand(
          catalog,
          None,
          saveMode,
          Some(CloneTableCommand(
            sourceTbl,
            tblIdent,
            statement.tablePropertyOverrides,
            path)),
          tableByPath = true,
          output = CloneTableCommand.output)

      // Target is a metastore table
      case UnresolvedRelation(SessionCatalogAndIdentifier(catalog, ident), _, _) =>
        if (!isCreate) {
          throw DeltaErrors.cannotReplaceMissingTableException(ident)
        }
        val tblIdent = ident
          .asTableIdentifier
        val finalTarget = new Path(statement.targetLocation.getOrElse(
          session.sessionState.catalog.defaultTablePath(tblIdent).toString))
        val catalogTable = createCatalogTableForCloneCommand(finalTarget, byPath = false, tblIdent,
          targetLocation, sourceCatalogTable, sourceTbl, statement.tablePropertyOverrides)
        val catalogTableWithPath = if (targetLocation.isEmpty) {
          catalogTable.copy(
            storage = CatalogStorageFormat.empty.copy(locationUri = Some(finalTarget.toUri)))
        } else {
          catalogTable
        }
        CreateDeltaTableCommand(
          catalogTableWithPath,
          None,
          saveMode,
          Some(CloneTableCommand(
            sourceTbl,
            tblIdent,
            statement.tablePropertyOverrides,
            finalTarget)),
          operation = tableCreationMode,
          output = CloneTableCommand.output)

      case UnresolvedRelation(NonSessionCatalogAndIdentifier(catalog: TableCatalog, ident), _, _) =>
        if (!isCreate) {
          throw DeltaErrors.cannotReplaceMissingTableException(ident)
        }
        val partitions: Array[Transform] = sourceTbl.metadata.partitionColumns.map { col =>
          new IdentityTransform(new FieldReference(Seq(col)))
        }.toArray
        // HACK ALERT: since there is no DSV2 API for getting table path before creation,
        //             here we create a table to get the path, then overwrite it with the
        //             cloned table.
        val sourceConfig = sourceTbl.metadata.configuration.asJava
        val newTable = catalog.createTable(ident, sourceTbl.schema, partitions, sourceConfig)
        try {
          newTable match {
            case targetTable: DeltaTableV2 =>
              val path = targetTable.path
              val tblIdent = TableIdentifier(path.toString, Some("delta"))
              val catalogTable = createCatalogTableForCloneCommand(path, byPath = true, tblIdent,
                targetLocation, sourceCatalogTable, sourceTbl, statement.tablePropertyOverrides)
              CreateDeltaTableCommand(
                table = catalogTable,
                existingTableOpt = None,
                mode = SaveMode.Overwrite,
                query = Some(
                  CloneTableCommand(
                    sourceTable = sourceTbl,
                    targetIdent = tblIdent,
                    tablePropertyOverrides = statement.tablePropertyOverrides,
                    targetPath = path)),
                tableByPath = true,
                operation = TableCreationModes.Replace,
                output = CloneTableCommand.output)
            case _ =>
              throw DeltaErrors.notADeltaSourceException("CREATE TABLE CLONE", Some(statement))
          }
        } catch {
          case NonFatal(e) =>
            catalog.dropTable(ident)
            throw e
        }
      // Delta metastore table already exists at target
      case DataSourceV2Relation(deltaTableV2: DeltaTableV2, _, _, _, _) =>
        val path = deltaTableV2.path
        val existingTable = deltaTableV2.catalogTable
        val tblIdent = existingTable match {
          case Some(existingCatalog) => existingCatalog.identifier
          case None => TableIdentifier(path.toString, Some("delta"))
        }
        val catalogTable = createCatalogTableForCloneCommand(
          path,
          byPath = existingTable.isEmpty,
          tblIdent,
          targetLocation,
          sourceCatalogTable,
          sourceTbl,
          statement.tablePropertyOverrides)

        CreateDeltaTableCommand(
          catalogTable,
          existingTable,
          saveMode,
          Some(CloneTableCommand(
            sourceTbl,
            tblIdent,
            statement.tablePropertyOverrides,
            path)),
          tableByPath = existingTable.isEmpty,
          operation = tableCreationMode,
          output = CloneTableCommand.output)

      // Non-delta metastore table already exists at target
      case LogicalRelationWithTable(_, existingCatalogTable @ Some(catalogTable)) =>
        val tblIdent = catalogTable.identifier
        val path = new Path(catalogTable.location)
        val newCatalogTable = createCatalogTableForCloneCommand(path, byPath = false, tblIdent,
          targetLocation, sourceCatalogTable, sourceTbl, statement.tablePropertyOverrides)
        CreateDeltaTableCommand(
          newCatalogTable,
          existingCatalogTable,
          saveMode,
          Some(CloneTableCommand(
            sourceTbl,
            tblIdent,
            statement.tablePropertyOverrides,
            path)),
          operation = tableCreationMode,
          output = CloneTableCommand.output)

      case _ => throw DeltaErrors.notADeltaTableException("CLONE")
    }
  }

  /**
   * Performs the schema adjustment by adding UpCasts (which are safe) and Aliases so that we
   * can check if the by-ordinal schema of the insert query matches our Delta table.
   * The schema adjustment also include string length check if it's written into a char/varchar
   * type column/field.
   */
  private def resolveQueryColumnsByOrdinal(
      query: LogicalPlan,
      targetAttrs: Seq[Attribute],
      deltaTable: DeltaTableV2,
      writeOptions: Map[String, String]): LogicalPlan = {
    // always add a Cast. it will be removed in the optimizer if it is unnecessary.
    val project = query.output.zipWithIndex.map { case (attr, i) =>
      if (i < targetAttrs.length) {
        val targetAttr = targetAttrs(i)
        addCastToColumn(attr, targetAttr, deltaTable.name(),
          typeWideningMode = getTypeWideningMode(deltaTable, writeOptions)
        )
      } else {
        attr
      }
    }
    Project(project, query)
  }

  /**
   * Performs the schema adjustment by adding UpCasts (which are safe) so that we can insert into
   * the Delta table when the input data types doesn't match the table schema. Unlike
   * `resolveQueryColumnsByOrdinal` which ignores the names in `targetAttrs` and maps attributes
   * directly to query output, this method will use the names in the query output to find the
   * corresponding attribute to use. This method also allows users to not provide values for
   * generated columns. If values of any columns are not in the query output, they must be generated
   * columns.
   */
  private def resolveQueryColumnsByName(
      query: LogicalPlan,
      targetAttrs: Seq[Attribute],
      deltaTable: DeltaTableV2,
      writeOptions: Map[String, String]): LogicalPlan = {
    insertIntoByNameMissingColumn(query, targetAttrs, deltaTable)

    // This is called before resolveOutputColumns in postHocResolutionRules, so we need to duplicate
    // the schema validation here.
    if (query.output.length > targetAttrs.length) {
      throw QueryCompilationErrors.cannotWriteTooManyColumnsToTableError(
        tableName = deltaTable.name(),
        expected = targetAttrs.map(_.name),
        queryOutput = query.output)
    }

    val project = query.output.map { attr =>
      val targetAttr = targetAttrs.find(t => session.sessionState.conf.resolver(t.name, attr.name))
        .getOrElse {
          throw DeltaErrors.missingColumn(attr, targetAttrs)
        }
      addCastToColumn(attr, targetAttr, deltaTable.name(),
        typeWideningMode = getTypeWideningMode(deltaTable, writeOptions)
      )
    }
    Project(project, query)
  }

  private def addCastToColumn(
      attr: NamedExpression,
      targetAttr: NamedExpression,
      tblName: String,
      typeWideningMode: TypeWideningMode): NamedExpression = {
    val expr = (attr.dataType, targetAttr.dataType) match {
      case (s, t) if s == t =>
        attr
      case (s: StructType, t: StructType) if s != t =>
        addCastsToStructs(tblName, attr, s, t, typeWideningMode)
      case (ArrayType(s: StructType, sNull: Boolean), ArrayType(t: StructType, tNull: Boolean))
        if s != t && sNull == tNull =>
        addCastsToArrayStructs(tblName, attr, s, t, sNull, typeWideningMode)
      case (s: AtomicType, t: AtomicType)
        if typeWideningMode.shouldWidenTo(fromType = t, toType = s) =>
        // Keep the type from the query, the target schema will be updated to widen the existing
        // type to match it.
        attr
      case (s: MapType, t: MapType)
        if !DataType.equalsStructurally(s, t, ignoreNullability = true) =>
        // only trigger addCastsToMaps if exists differences like extra fields, renaming or type
        // differences.
        addCastsToMaps(tblName, attr, s, t, typeWideningMode)
      case _ =>
        getCastFunction(attr, targetAttr.dataType, targetAttr.name)
    }
    Alias(expr, targetAttr.name)(explicitMetadata = Option(targetAttr.metadata))
  }

  /**
   * Returns the type widening mode to use for the given delta table. A type widening mode indicates
   * for (fromType, toType) tuples whether `fromType` is eligible to be automatically widened to
   * `toType` when ingesting data. If it is, the table schema is updated to `toType` before
   * ingestion and values are written using their original `toType` type. Otherwise, the table type
   * `fromType` is retained and values are downcasted on write.
   */
  private def getTypeWideningMode(
      deltaTable: DeltaTableV2,
      writeOptions: Map[String, String]): TypeWideningMode = {
    val options = new DeltaOptions(deltaTable.options ++ writeOptions, conf)
    val snapshot = deltaTable.initialSnapshot
    val typeWideningEnabled = TypeWidening.isEnabled(snapshot.protocol, snapshot.metadata)
    val schemaEvolutionEnabled = options.canMergeSchema

    if (typeWideningEnabled && schemaEvolutionEnabled) {
      TypeWideningMode.TypeEvolution(
        uniformIcebergCompatibleOnly = UniversalFormat.icebergEnabled(snapshot.metadata))
    } else {
      TypeWideningMode.NoTypeWidening
    }
  }

  /**
   * With Delta, we ACCEPT_ANY_SCHEMA, meaning that Spark doesn't automatically adjust the schema
   * of INSERT INTO. This allows us to perform better schema enforcement/evolution. Since Spark
   * skips this step, we see if we need to perform any schema adjustment here.
   */
  private def needsSchemaAdjustmentByOrdinal(
      deltaTable: DeltaTableV2,
      query: LogicalPlan,
      schema: StructType,
      writeOptions: Map[String, String]): Boolean = {
    val output = query.output
    if (output.length < schema.length) {
      throw DeltaErrors.notEnoughColumnsInInsert(deltaTable.name(), output.length, schema.length)
    }
    // Now we should try our best to match everything that already exists, and leave the rest
    // for schema evolution to WriteIntoDelta
    val existingSchemaOutput = output.take(schema.length)
    existingSchemaOutput.map(_.name) != schema.map(_.name) ||
      !SchemaUtils.isReadCompatible(schema.asNullable, existingSchemaOutput.toStructType,
        typeWideningMode = getTypeWideningMode(deltaTable, writeOptions))
  }

  /**
   * Checks for missing columns in a insert by name query and throws an exception if found.
   * Delta does not require users to provide values for generated columns, so any columns missing
   * from the query output must have a default expression.
   * See [[ColumnWithDefaultExprUtils.columnHasDefaultExpr]].
   */
  private def insertIntoByNameMissingColumn(
      query: LogicalPlan,
      targetAttrs: Seq[Attribute],
      deltaTable: DeltaTableV2): Unit = {
    if (query.output.length < targetAttrs.length) {
      // Some columns are not specified. We don't allow schema evolution in INSERT INTO BY NAME, so
      // we need to ensure the missing columns must be generated columns.
      val userSpecifiedNames = if (session.sessionState.conf.caseSensitiveAnalysis) {
        query.output.map(a => (a.name, a)).toMap
      } else {
        CaseInsensitiveMap(query.output.map(a => (a.name, a)).toMap)
      }
      val tableSchema = deltaTable.initialSnapshot.metadata.schema
      if (tableSchema.length != targetAttrs.length) {
        // The target attributes may contain the metadata columns by design. Throwing an exception
        // here in case target attributes may have the metadata columns for Delta in future.
        throw DeltaErrors.schemaNotConsistentWithTarget(s"$tableSchema", s"$targetAttrs")
      }
      val nullAsDefault = deltaTable.spark.sessionState.conf.useNullsForMissingDefaultColumnValues
      deltaTable.initialSnapshot.metadata.schema.foreach { col =>
        if (!userSpecifiedNames.contains(col.name) &&
          !ColumnWithDefaultExprUtils.columnHasDefaultExpr(
            deltaTable.initialSnapshot.protocol, col, nullAsDefault)) {
          throw DeltaErrors.missingColumnsInInsertInto(col.name)
        }
      }
    }
  }

  /**
   * With Delta, we ACCEPT_ANY_SCHEMA, meaning that Spark doesn't automatically adjust the schema
   * of INSERT INTO. Here we check if we need to perform any schema adjustment for INSERT INTO by
   * name queries. We also check that any columns not in the list of user-specified columns must
   * have a default expression.
   */
  private def needsSchemaAdjustmentByName(
      query: LogicalPlan,
      targetAttrs: Seq[Attribute],
      deltaTable: DeltaTableV2,
      writeOptions: Map[String, String]): Boolean = {
    insertIntoByNameMissingColumn(query, targetAttrs, deltaTable)
    val userSpecifiedNames = if (session.sessionState.conf.caseSensitiveAnalysis) {
      query.output.map(a => (a.name, a)).toMap
    } else {
      CaseInsensitiveMap(query.output.map(a => (a.name, a)).toMap)
    }
    val specifiedTargetAttrs = targetAttrs.filter(col => userSpecifiedNames.contains(col.name))
    !SchemaUtils.isReadCompatible(
      specifiedTargetAttrs.toStructType.asNullable,
      query.output.toStructType,
      typeWideningMode = getTypeWideningMode(deltaTable, writeOptions)
    )
  }

  // Get cast operation for the level of strictness in the schema a user asked for
  private def getCastFunction: CastFunction = {
    val timeZone = conf.sessionLocalTimeZone
    conf.storeAssignmentPolicy match {
      case SQLConf.StoreAssignmentPolicy.LEGACY =>
        (input: Expression, dt: DataType, _) =>
          Cast(input, dt, Option(timeZone), ansiEnabled = false)
      case SQLConf.StoreAssignmentPolicy.ANSI =>
        (input: Expression, dt: DataType, name: String) => {
          val cast = Cast(input, dt, Option(timeZone), ansiEnabled = true)
          cast.setTagValue(Cast.BY_TABLE_INSERTION, ())
          TableOutputResolver.checkCastOverflowInTableInsert(cast, name)
        }
      case SQLConf.StoreAssignmentPolicy.STRICT =>
        (input: Expression, dt: DataType, _) =>
          UpCast(input, dt)
    }
  }

  /**
   * Recursively casts struct data types in case the source/target type differs.
   */
  private def addCastsToStructs(
      tableName: String,
      parent: NamedExpression,
      source: StructType,
      target: StructType,
      typeWideningMode: TypeWideningMode): NamedExpression = {
    if (source.length < target.length) {
      throw DeltaErrors.notEnoughColumnsInInsert(
        tableName, source.length, target.length, Some(parent.qualifiedName))
    }
    // Extracts the field at a given index in the target schema. Only matches if the index is valid.
    object TargetIndex {
      def unapply(index: Int): Option[StructField] = target.lift(index)
    }

    val fields = source.zipWithIndex.map {
      case (StructField(name, nested: StructType, _, metadata), i @ TargetIndex(targetField)) =>
        targetField.dataType match {
          case t: StructType =>
            val subField = Alias(GetStructField(parent, i, Option(name)), targetField.name)(
              explicitMetadata = Option(metadata))
            addCastsToStructs(tableName, subField, nested, t, typeWideningMode)
          case o =>
            val field = parent.qualifiedName + "." + name
            val targetName = parent.qualifiedName + "." + targetField.name
            throw DeltaErrors.cannotInsertIntoColumn(tableName, field, targetName, o.simpleString)
        }

      case (StructField(name, sourceType: AtomicType, _, _),
            i @ TargetIndex(StructField(targetName, targetType: AtomicType, _, targetMetadata)))
          if typeWideningMode.shouldWidenTo(fromType = targetType, toType = sourceType) =>
        Alias(
          GetStructField(parent, i, Option(name)),
          targetName)(explicitMetadata = Option(targetMetadata))
      case (sourceField, i @ TargetIndex(targetField)) =>
        Alias(
          getCastFunction(GetStructField(parent, i, Option(sourceField.name)),
            targetField.dataType, targetField.name),
          targetField.name)(explicitMetadata = Option(targetField.metadata))

      case (sourceField, i) =>
        // This is a new column, so leave to schema evolution as is. Do not lose it's name so
        // wrap with an alias
        Alias(
          GetStructField(parent, i, Option(sourceField.name)),
          sourceField.name)(explicitMetadata = Option(sourceField.metadata))
    }
    Alias(CreateStruct(fields), parent.name)(
      parent.exprId, parent.qualifier, Option(parent.metadata))
  }

  private def addCastsToArrayStructs(
      tableName: String,
      parent: NamedExpression,
      source: StructType,
      target: StructType,
      sourceNullable: Boolean,
      typeWideningMode: TypeWideningMode): Expression = {
    val structConverter: (Expression, Expression) => Expression = (_, i) =>
      addCastsToStructs(
        tableName, Alias(GetArrayItem(parent, i), i.toString)(), source, target, typeWideningMode)
    val transformLambdaFunc = {
      val elementVar = NamedLambdaVariable("elementVar", source, sourceNullable)
      val indexVar = NamedLambdaVariable("indexVar", IntegerType, false)
      LambdaFunction(structConverter(elementVar, indexVar), Seq(elementVar, indexVar))
    }
    ArrayTransform(parent, transformLambdaFunc)
  }

  private def stripTempViewWrapper(plan: LogicalPlan): LogicalPlan = {
    DeltaViewHelper.stripTempView(plan, conf)
  }

  private def stripTempViewForMergeWrapper(plan: LogicalPlan): LogicalPlan = {
    DeltaViewHelper.stripTempViewForMerge(plan, conf)
  }

  /**
   * Recursively casts map data types in case the key/value type differs.
   */
  private def addCastsToMaps(
      tableName: String,
      parent: NamedExpression,
      sourceMapType: MapType,
      targetMapType: MapType,
      typeWideningMode: TypeWideningMode): Expression = {
    val transformedKeys =
      if (sourceMapType.keyType != targetMapType.keyType) {
        // Create a transformation for the keys
        ArrayTransform(MapKeys(parent), {
          val key = NamedLambdaVariable(
            "key", sourceMapType.keyType, nullable = false)

          val keyAttr = AttributeReference(
            "key", targetMapType.keyType, nullable = false)()

          val castedKey =
            addCastToColumn(
              key,
              keyAttr,
              tableName,
              typeWideningMode
            )
          LambdaFunction(castedKey, Seq(key))
        })
      } else {
        MapKeys(parent)
      }

    val transformedValues =
      if (sourceMapType.valueType != targetMapType.valueType) {
        // Create a transformation for the values
        ArrayTransform(MapValues(parent), {
          val value = NamedLambdaVariable(
            "value", sourceMapType.valueType, sourceMapType.valueContainsNull)

          val valueAttr = AttributeReference(
            "value", targetMapType.valueType, sourceMapType.valueContainsNull)()

          val castedValue =
            addCastToColumn(
              value,
              valueAttr,
              tableName,
              typeWideningMode
            )
          LambdaFunction(castedValue, Seq(value))
        })
      } else {
        MapValues(parent)
      }
    // Create new map from transformed keys and values
    MapFromArrays(transformedKeys, transformedValues)
  }

  /**
   * Verify the input plan for a SINGLE streaming query with the following:
   * 1. Schema location must be under checkpoint location, if not lifted by flag
   * 2. No two duplicating delta source can share the same schema location
   */
  private def verifyDeltaSourceSchemaLocation(
      inputQuery: LogicalPlan,
      checkpointLocation: String): Unit = {
    // Maps StreamingRelation to schema location, similar to how MicroBatchExecution converts
    // StreamingRelation to StreamingExecutionRelation.
    val schemaLocationMap = mutable.Map[StreamingRelation, String]()
    val allowSchemaLocationOutsideOfCheckpoint = session.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_STREAMING_ALLOW_SCHEMA_LOCATION_OUTSIDE_CHECKPOINT_LOCATION)
    inputQuery.foreach {
      case streamingRelation @ StreamingRelation(dataSourceV1, sourceName, _)
        if DeltaSourceUtils.isDeltaDataSourceName(sourceName) =>
          DeltaDataSource.extractSchemaTrackingLocationConfig(
            session, dataSourceV1.options
          ).foreach { rootSchemaTrackingLocation =>
            assert(dataSourceV1.options.contains("path"), "Path for Delta table must be defined")
            val tableId =
              dataSourceV1.options("path").replace(":", "").replace("/", "_")
            val sourceIdOpt = dataSourceV1.options.get(DeltaOptions.STREAMING_SOURCE_TRACKING_ID)
            val schemaTrackingLocation =
              DeltaSourceMetadataTrackingLog.fullMetadataTrackingLocation(
                rootSchemaTrackingLocation, tableId, sourceIdOpt)
            // Make sure schema location is under checkpoint
            if (!allowSchemaLocationOutsideOfCheckpoint) {
              assertSchemaTrackingLocationUnderCheckpoint(
                checkpointLocation,
                schemaTrackingLocation
              )
            }
            // Save schema location for this streaming relation
            schemaLocationMap.put(streamingRelation, schemaTrackingLocation.stripSuffix("/"))
          }
      case _ =>
    }

    // Now verify all schema locations are distinct
    val conflictSchemaOpt = schemaLocationMap
      .keys
      .groupBy { rel => schemaLocationMap(rel) }
      .find(_._2.size > 1)
    conflictSchemaOpt.foreach { case (schemaLocation, relations) =>
      val ds = relations.head.dataSource
      // Pick one source that has conflict to make it more actionable for the user
      val oneTableWithConflict = ds.catalogTable
        .map(_.identifier.toString)
        .getOrElse {
          // `path` must exist
          CaseInsensitiveMap(ds.options).get("path").get
        }
      throw DeltaErrors.sourcesWithConflictingSchemaTrackingLocation(
        schemaLocation, oneTableWithConflict)
    }
  }

  /**
   * Check and assert whether the schema tracking location is under the checkpoint location.
   *
   * Visible for testing.
   */
  private[delta] def assertSchemaTrackingLocationUnderCheckpoint(
      checkpointLocation: String,
      schemaTrackingLocation: String): Unit = {
    val checkpointPath = new Path(checkpointLocation)
    // scalastyle:off deltahadoopconfiguration
    val checkpointFs = checkpointPath.getFileSystem(session.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration
    val qualifiedCheckpointPath = checkpointFs.makeQualified(checkpointPath)
    val qualifiedSchemaTrackingLocationPath = try {
      checkpointFs.makeQualified(new Path(schemaTrackingLocation))
    } catch {
      case NonFatal(e) =>
        // This can happen when the file system for the checkpoint location is completely different
        // from that of the schema tracking location.
        logWarning("Failed to make a qualified path for schema tracking location", e)
        throw DeltaErrors.schemaTrackingLocationNotUnderCheckpointLocation(
          schemaTrackingLocation, checkpointLocation)
    }
    // If we couldn't qualify the schema location or after relativization, the result is still an
    // absolute path, we know the schema location is not under checkpoint.
    if (qualifiedCheckpointPath.toUri.relativize(
        qualifiedSchemaTrackingLocationPath.toUri).isAbsolute) {
      throw DeltaErrors.schemaTrackingLocationNotUnderCheckpointLocation(
        schemaTrackingLocation, checkpointLocation)
    }
  }

  object EligibleCreateTableLikeCommand {
    def unapply(arg: LogicalPlan): Option[(CreateTableLikeCommand, CatalogTable)] = arg match {
      case c: CreateTableLikeCommand =>
        val src = session.sessionState.catalog.getTempViewOrPermanentTableMetadata(c.sourceTable)
        if (src.provider.contains("delta") ||
          c.provider.exists(DeltaSourceUtils.isDeltaDataSourceName)) {
          Some(c, src)
        } else {
          None
        }
      case _ =>
        None
    }
  }
}

/** Matchers for dealing with a Delta table. */
object DeltaRelation extends DeltaLogging {
  val KEEP_AS_V2_RELATION_TAG = new TreeNodeTag[Unit]("__keep_as_v2_relation")

  def unapply(plan: LogicalPlan): Option[LogicalRelation] = plan match {
    case dsv2 @ DataSourceV2Relation(d: DeltaTableV2, _, _, _, options) =>
      Some(fromV2Relation(d, dsv2, options))
    case lr @ DeltaTable(_) => Some(lr)
    case _ => None
  }

  def fromV2Relation(
      d: DeltaTableV2,
      v2Relation: DataSourceV2Relation,
      options: CaseInsensitiveStringMap): LogicalRelation = {
    recordFrameProfile("DeltaAnalysis", "fromV2Relation") {
      val relation = d.withOptions(options.asScala.toMap).toBaseRelation
      val output = if (CDCReader.isCDCRead(options)) {
        // Handles cdc for the spark.read.options().table() code path
        // Mapping needed for references to the table's columns coming from Spark Connect.
        val newOutput = toAttributes(relation.schema)
        newOutput.map { a =>
          val existingReference = v2Relation.output
            .find(e => e.name == a.name && e.dataType == a.dataType && e.nullable == a.nullable)
          existingReference.map { e =>
            e.copy(metadata = a.metadata)(exprId = e.exprId, qualifier = e.qualifier)
          }.getOrElse(a)
        }
      } else {
        v2Relation.output
      }
      LogicalRelationShims.newInstance(relation, output, d.ttSafeCatalogTable, isStreaming = false)
    }
  }
}

object AppendDelta {
  def unapply(a: AppendData): Option[(DataSourceV2Relation, DeltaTableV2)] = {
    if (a.query.resolved) {
      a.table match {
        case r: DataSourceV2Relation if r.table.isInstanceOf[DeltaTableV2] =>
          Some((r, r.table.asInstanceOf[DeltaTableV2]))
        case _ => None
      }
    } else {
      None
    }
  }
}

object OverwriteDelta {
  def unapply(o: OverwriteByExpression): Option[(DataSourceV2Relation, DeltaTableV2)] = {
    if (o.query.resolved) {
      o.table match {
        case r: DataSourceV2Relation if r.table.isInstanceOf[DeltaTableV2] =>
          Some((r, r.table.asInstanceOf[DeltaTableV2]))
        case _ => None
      }
    } else {
      None
    }
  }
}

object DynamicPartitionOverwriteDelta {
  def unapply(o: OverwritePartitionsDynamic): Option[(DataSourceV2Relation, DeltaTableV2)] = {
    if (o.query.resolved) {
      o.table match {
        case r: DataSourceV2Relation if r.table.isInstanceOf[DeltaTableV2] =>
          Some((r, r.table.asInstanceOf[DeltaTableV2]))
        case _ => None
      }
    } else {
      None
    }
  }
}

/**
 * A `RunnableCommand` that will execute dynamic partition overwrite using [[WriteIntoDelta]].
 *
 * This is a workaround of Spark not supporting V1 fallback for dynamic partition overwrite.
 * Note the following details:
 * - Extends `V2WriteCommmand` so that Spark can transform this plan in the same as other
 *   commands like `AppendData`.
 * - Exposes the query as a child so that the Spark optimizer can optimize it.
 */
case class DeltaDynamicPartitionOverwriteCommand(
    table: NamedRelation,
    deltaTable: DeltaTableV2,
    query: LogicalPlan,
    writeOptions: Map[String, String],
    isByName: Boolean,
    analyzedQuery: Option[LogicalPlan] = None) extends RunnableCommand with V2WriteCommand {

  override def child: LogicalPlan = query

  override def withNewQuery(newQuery: LogicalPlan): DeltaDynamicPartitionOverwriteCommand = {
    copy(query = newQuery)
  }

  override def withNewTable(newTable: NamedRelation): DeltaDynamicPartitionOverwriteCommand = {
    copy(table = newTable)
  }

  override def storeAnalyzedQuery(): Command = copy(analyzedQuery = Some(query))

  override protected def withNewChildInternal(
      newChild: LogicalPlan): DeltaDynamicPartitionOverwriteCommand = copy(query = newChild)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaOptions = new DeltaOptions(
      CaseInsensitiveMap[String](
        deltaTable.options ++
        writeOptions ++
        Seq(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION ->
          DeltaOptions.PARTITION_OVERWRITE_MODE_DYNAMIC)),
      sparkSession.sessionState.conf)

    // TODO: The configuration can be fetched directly from WriteIntoDelta's txn. Don't pass
    //  in the default snapshot's metadata config here.
    WriteIntoDelta(
      deltaTable.deltaLog,
      SaveMode.Overwrite,
      deltaOptions,
      partitionColumns = Nil,
      deltaTable.deltaLog.unsafeVolatileSnapshot.metadata.configuration,
      DataFrameUtils.ofRows(sparkSession, query),
      deltaTable.catalogTable
    ).run(sparkSession)
  }
}

