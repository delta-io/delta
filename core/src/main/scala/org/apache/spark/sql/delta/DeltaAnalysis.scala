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

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.catalyst.TimeTravel
import org.apache.spark.sql.delta.DeltaErrors.{TemporallyUnstableInputException, TimestampEarlierThanCommitRetentionException}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.catalog.IcebergTablePlaceHolder
import org.apache.spark.sql.delta.commands._
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.constraints.{AddConstraint, DropConstraint}
import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources._
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.analysis.UnresolvedTableValuedFunction
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.CloneTableStatement
import org.apache.spark.sql.catalyst.plans.logical.RestoreTableStatement
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.WriteToStream
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.CreateTableLikeCommand
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, StructField, StructType}
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
        needsSchemaAdjustmentByOrdinal(d.name(), a.query, r.schema) =>
      val projection = resolveQueryColumnsByOrdinal(a.query, r.output, d.name())
      if (projection != a.query) {
        a.copy(query = projection)
      } else {
        a
      }


    // INSERT INTO by name
    // AppendData.byName is also used for DataFrame append so we check for the SQL origin text
    // since we only want to up-cast for SQL insert into by name
    case a @ AppendDelta(r, d) if a.isByName &&
        a.origin.sqlText.nonEmpty && needsSchemaAdjustmentByName(a.query, r.output, d) =>
      val projection = resolveQueryColumnsByName(a.query, r.output, d)
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

      val catalogTableTarget =
        // If source table is Delta format
        if (src.provider.exists(_.toLowerCase() == "delta")) {
          val deltaLogSrc = DeltaTableV2(session, new Path(src.location))

          // maxColumnId field cannot be set externally. If column-mapping is
          // used on the source delta table, then maxColumnId would be set for the sourceTable
          // and needs to be removed from the targetTable's configuration
          // maxColumnId will be set in the targetTable's configuration internally after
          val config =
            deltaLogSrc.snapshot.metadata.configuration.-("delta.columnMapping.maxColumnId")

          new CatalogTable(
            identifier = targetTableIdentifier,
            tableType = tblType,
            storage = newStorage,
            schema = deltaLogSrc.snapshot.metadata.schema,
            properties = config,
            partitionColumnNames = deltaLogSrc.snapshot.metadata.partitionColumns,
            provider = Some("delta"),
            comment = Option(deltaLogSrc.snapshot.metadata.description)
          )
        } else { // Source table is not delta format
            new CatalogTable(
              identifier = targetTableIdentifier,
              tableType = tblType,
              storage = newStorage,
              schema = src.schema,
              properties = src.properties,
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
        if (src.provider == Some("delta")) {
          Some(DeltaTableV2(session, new Path(src.location)).snapshot.protocol)
        } else {
          None
        }
      val newDeltaCatalog = new DeltaCatalog()
      CreateDeltaTableCommand(
        table = newDeltaCatalog.verifyTableAndSolidify(catalogTableTarget, None),
        existingTableOpt = newDeltaCatalog.getExistingTableIfExists(catalogTableTarget.identifier),
        mode = saveMode,
        query = None,
        output = ctl.output,
        protocol = protocol,
        tableByPath = isTableByPath)

    // INSERT OVERWRITE by ordinal and df.insertInto()
    case o @ OverwriteDelta(r, d) if !o.isByName &&
        needsSchemaAdjustmentByOrdinal(d.name(), o.query, r.schema) =>
      val projection = resolveQueryColumnsByOrdinal(o.query, r.output, d.name())
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
    case o @ OverwriteDelta(r, d) if o.isByName &&
        o.origin.sqlText.nonEmpty && needsSchemaAdjustmentByName(o.query, r.output, d) =>
      val projection = resolveQueryColumnsByName(o.query, r.output, d)
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
          needsSchemaAdjustmentByOrdinal(d.name(), o.query, r.schema)) {
        // INSERT OVERWRITE by ordinal and df.insertInto()
        resolveQueryColumnsByOrdinal(o.query, r.output, d.name())
      } else if (o.isByName && o.origin.sqlText.nonEmpty &&
          needsSchemaAdjustmentByName(o.query, r.output, d)) {
        // INSERT OVERWRITE by name
        // OverwriteDelta.byName is also used for DataFrame append so we check for the SQL origin
        // text since we only want to up-cast for SQL insert into by name
        resolveQueryColumnsByName(o.query, r.output, d)
      } else {
        o.query
      }
      DeltaDynamicPartitionOverwriteCommand(r, d, adjustedQuery, o.writeOptions, o.isByName)

    // Pull out the partition filter that may be part of the FileIndex. This can happen when someone
    // queries a Delta table such as spark.read.format("delta").load("/some/table/partition=2")
    case l @ DeltaTable(index: TahoeLogFileIndex) if index.partitionFilters.nonEmpty =>
      Filter(
        index.partitionFilters.reduce(And),
        DeltaTableUtils.replaceFileIndex(l, index.copy(partitionFilters = Nil)))

    // SQL CDC table value functions "table_changes" and "table_changes_by_path"
    case t: DeltaTableValueFunction if t.functionArgs.forall(_.resolved)
    =>
      DeltaTableValueFunctions.resolveChangesTableValueFunctions(
        session,
        t.fnName,
        t.functionArgs
      )


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
              table.tableIdentifier, sparkTable = None, tableSchema = None, session),
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
            CloneIcebergSource(tableIdent, Some(table), tableSchema = None, session),
            cloneStatement)

        case u: UnresolvedRelation =>
          u.tableNotFound(u.multipartIdentifier)

        case TimeTravel(u: UnresolvedRelation, _, _, _) =>
          u.tableNotFound(u.multipartIdentifier)

        case LogicalRelation(
            HadoopFsRelation(location, _, _, _, _: ParquetFileFormat, _), _, catalogTable, _) =>
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
          val tblIdent = tbl.catalogTable match {
            case Some(existingCatalog) => existingCatalog.identifier
            case None => TableIdentifier(tbl.path.toString, Some("delta"))
          }
          // restoring to same version as latest should be a no-op.
          val sourceSnapshot = try {
            traveledTable.snapshot
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

          RestoreTableCommand(traveledTable, tblIdent)

        case u: UnresolvedRelation =>
          u.tableNotFound(u.multipartIdentifier)

        case TimeTravel(u: UnresolvedRelation, _, _, _) =>
          u.tableNotFound(u.multipartIdentifier)

        case _ =>
          throw DeltaErrors.notADeltaTableException("RESTORE")
      }

    // This rule falls back to V1 nodes, since we don't have a V2 reader for Delta right now
    case dsv2 @ DataSourceV2Relation(d: DeltaTableV2, _, _, _, options) =>
      DeltaRelation.fromV2Relation(d, dsv2, options)

    // DML - TODO: Remove these Delta-specific DML logical plans and use Spark's plans directly

    case d @ DeleteFromTable(table, condition) if d.childrenResolved =>
      // rewrites Delta from V2 to V1
      val newTarget = stripTempViewWrapper(table).transformUp { case DeltaRelation(lr) => lr }
      val indices = newTarget.collect {
        case DeltaFullTable(index) => index
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
          case Some(DeltaFullTable(index)) =>
          case o =>
            throw DeltaErrors.notADeltaSourceException("UPDATE", o)
        }
      DeltaUpdateTable(newTable, cols, expressions, condition)

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
      val newTarget =
        stripTempViewForMergeWrapper(merge.targetTable).transformUp { case DeltaRelation(lr) => lr }
      // Even if we're merging into a non-Delta target, we will catch it later and throw an
      // exception.
      val deltaMerge = DeltaMergeInto(
        newTarget,
        merge.sourceTable,
        merge.mergeCondition,
        matchedActions ++ notMatchedActions ++ notMatchedBySourceActions
      )

      DeltaMergeInto.resolveReferencesAndSchema(deltaMerge, conf)(tryResolveReferences(session))

    case reorg@DeltaReorgTable(_@ResolvedTable(_, _, t, _)) =>
      t match {
        case table: DeltaTableV2 =>
          DeltaReorgTableCommand(table)(reorg.predicates)
        case _ =>
          throw DeltaErrors.notADeltaTable(t.name())
      }

    case deltaMerge: DeltaMergeInto =>
      val d = if (deltaMerge.childrenResolved && !deltaMerge.resolved) {
        DeltaMergeInto.resolveReferencesAndSchema(deltaMerge, conf)(tryResolveReferences(session))
      } else deltaMerge
      d.copy(target = stripTempViewForMergeWrapper(d.target))

    case streamWrite: WriteToStream =>
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
      srcTable: CloneSource): CatalogTable = {
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
    val properties = srcTable.metadata.configuration

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

    import session.sessionState.analyzer.SessionCatalogAndIdentifier
    val targetLocation = statement.targetLocation
    val saveMode = if (isReplace) {
      SaveMode.Overwrite
    } else if (statement.ifNotExists) {
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
    // We don't use information in the catalog if the table is time travelled
    val sourceCatalogTable = if (sourceTbl.timeTravelOpt.isDefined) None else sourceTbl.catalogTable

    EliminateSubqueryAliases(targetPlan) match {
      // Target is a path based table
      case DataSourceV2Relation(targetTbl @ DeltaTableV2(_, path, _, _, _, _, _), _, _, _, _)
          if !targetTbl.tableExists =>
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
        val catalog = createCatalogTableForCloneCommand(
          path, byPath = true, tblIdent, targetLocation, sourceCatalogTable, sourceTbl)
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
        val catalogTable = createCatalogTableForCloneCommand(
          finalTarget, byPath = false, tblIdent, targetLocation, sourceCatalogTable, sourceTbl)
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

      // Delta metastore table already exists at target
      case DataSourceV2Relation(
          deltaTableV2 @ DeltaTableV2(_, path, existingTable, _, _, _, _), _, _, _, _) =>
        val tblIdent = existingTable match {
          case Some(existingCatalog) => existingCatalog.identifier
          case None => TableIdentifier(path.toString, Some("delta"))
        }
        // Reuse the existing schema so that the physical name of columns are consistent
        val cloneSourceTable = sourceTbl match {
          case source: CloneIcebergSource =>
            // Reuse the existing schema so that the physical name of columns are consistent
            source.copy(tableSchema = Some(deltaTableV2.snapshot.metadata.schema))
          case other => other
        }
        val catalogTable = createCatalogTableForCloneCommand(
          path,
          byPath = existingTable.isEmpty,
          tblIdent,
          targetLocation,
          sourceCatalogTable,
          cloneSourceTable)

        CreateDeltaTableCommand(
          catalogTable,
          existingTable,
          saveMode,
          Some(CloneTableCommand(
            cloneSourceTable,
            tblIdent,
            statement.tablePropertyOverrides,
            path)),
          tableByPath = existingTable.isEmpty,
          operation = tableCreationMode,
          output = CloneTableCommand.output)

      // Non-delta metastore table already exists at target
      case LogicalRelation(_, _, existingCatalogTable @ Some(catalogTable), _) =>
        val tblIdent = catalogTable.identifier
        val path = new Path(catalogTable.location)
        val newCatalogTable = createCatalogTableForCloneCommand(
          path, byPath = false, tblIdent, targetLocation, sourceCatalogTable, sourceTbl)
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
      query: LogicalPlan, targetAttrs: Seq[Attribute], tblName: String): LogicalPlan = {
    // always add a Cast. it will be removed in the optimizer if it is unnecessary.
    val project = query.output.zipWithIndex.map { case (attr, i) =>
      if (i < targetAttrs.length) {
        val targetAttr = targetAttrs(i)
        addCastToColumn(attr, targetAttr, tblName)
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
      query: LogicalPlan, targetAttrs: Seq[Attribute], deltaTable: DeltaTableV2): LogicalPlan = {
    insertIntoByNameMissingColumn(query, targetAttrs, deltaTable)
    // Spark will resolve columns to make sure specified columns are in the table schema and don't
    // have duplicates. This is just a sanity check.
    assert(
      query.output.length <= targetAttrs.length,
      s"Too many specified columns ${query.output.map(_.name).mkString(", ")}. " +
        s"Table columns: ${targetAttrs.map(_.name).mkString(", ")}")

    val project = query.output.map { attr =>
      val targetAttr = targetAttrs.find(t => session.sessionState.conf.resolver(t.name, attr.name))
        .getOrElse {
          // This is a sanity check. Spark should have done the check.
          throw DeltaErrors.missingColumn(attr, targetAttrs)
        }
      addCastToColumn(attr, targetAttr, deltaTable.name())
    }
    Project(project, query)
  }

  private def addCastToColumn(
      attr: Attribute,
      targetAttr: Attribute,
      tblName: String): NamedExpression = {
    val expr = (attr.dataType, targetAttr.dataType) match {
      case (s, t) if s == t =>
        attr
      case (s: StructType, t: StructType) if s != t =>
        addCastsToStructs(tblName, attr, s, t)
      case (ArrayType(s: StructType, sNull: Boolean), ArrayType(t: StructType, tNull: Boolean))
          if s != t && sNull == tNull =>
        addCastsToArrayStructs(tblName, attr, s, t, sNull)
      case _ =>
        getCastFunction(attr, targetAttr.dataType, targetAttr.name)
    }
    Alias(expr, targetAttr.name)(explicitMetadata = Option(targetAttr.metadata))
  }

  /**
   * With Delta, we ACCEPT_ANY_SCHEMA, meaning that Spark doesn't automatically adjust the schema
   * of INSERT INTO. This allows us to perform better schema enforcement/evolution. Since Spark
   * skips this step, we see if we need to perform any schema adjustment here.
   */
  private def needsSchemaAdjustmentByOrdinal(
      tableName: String,
      query: LogicalPlan,
      schema: StructType): Boolean = {
    val output = query.output
    if (output.length < schema.length) {
      throw DeltaErrors.notEnoughColumnsInInsert(tableName, output.length, schema.length)
    }
    // Now we should try our best to match everything that already exists, and leave the rest
    // for schema evolution to WriteIntoDelta
    val existingSchemaOutput = output.take(schema.length)
    existingSchemaOutput.map(_.name) != schema.map(_.name) ||
      !SchemaUtils.isReadCompatible(schema.asNullable, existingSchemaOutput.toStructType)
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
      val tableSchema = deltaTable.snapshot.metadata.schema
      if (tableSchema.length != targetAttrs.length) {
        // The target attributes may contain the metadata columns by design. Throwing an exception
        // here in case target attributes may have the metadata columns for Delta in future.
        throw DeltaErrors.schemaNotConsistentWithTarget(s"$tableSchema", s"$targetAttrs")
      }
      deltaTable.snapshot.metadata.schema.foreach { col =>
        if (!userSpecifiedNames.contains(col.name) &&
          !ColumnWithDefaultExprUtils.columnHasDefaultExpr(deltaTable.snapshot.protocol, col)) {
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
  private def needsSchemaAdjustmentByName(query: LogicalPlan, targetAttrs: Seq[Attribute],
      deltaTable: DeltaTableV2): Boolean = {
    insertIntoByNameMissingColumn(query, targetAttrs, deltaTable)
    val userSpecifiedNames = if (session.sessionState.conf.caseSensitiveAnalysis) {
      query.output.map(a => (a.name, a)).toMap
    } else {
      CaseInsensitiveMap(query.output.map(a => (a.name, a)).toMap)
    }
    val specifiedTargetAttrs = targetAttrs.filter(col => userSpecifiedNames.contains(col.name))
    !SchemaUtils.isReadCompatible(
      specifiedTargetAttrs.toStructType.asNullable, query.output.toStructType)
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
   * Recursively casts structs in case it contains null types.
   * TODO: Support other complex types like MapType and ArrayType
   */
  private def addCastsToStructs(
      tableName: String,
      parent: NamedExpression,
      source: StructType,
      target: StructType): NamedExpression = {
    if (source.length < target.length) {
      throw DeltaErrors.notEnoughColumnsInInsert(
        tableName, source.length, target.length, Some(parent.qualifiedName))
    }
    val fields = source.zipWithIndex.map {
      case (StructField(name, nested: StructType, _, metadata), i) if i < target.length =>
        target(i).dataType match {
          case t: StructType =>
            val subField = Alias(GetStructField(parent, i, Option(name)), target(i).name)(
              explicitMetadata = Option(metadata))
            addCastsToStructs(tableName, subField, nested, t)
          case o =>
            val field = parent.qualifiedName + "." + name
            val targetName = parent.qualifiedName + "." + target(i).name
            throw DeltaErrors.cannotInsertIntoColumn(tableName, field, targetName, o.simpleString)
        }
      case (other, i) if i < target.length =>
        val targetAttr = target(i)
        Alias(
          getCastFunction(GetStructField(parent, i, Option(other.name)),
            targetAttr.dataType, targetAttr.name),
          targetAttr.name)(explicitMetadata = Option(targetAttr.metadata))

      case (other, i) =>
        // This is a new column, so leave to schema evolution as is. Do not lose it's name so
        // wrap with an alias
        Alias(
          GetStructField(parent, i, Option(other.name)),
          other.name)(explicitMetadata = Option(other.metadata))
    }
    Alias(CreateStruct(fields), parent.name)(
      parent.exprId, parent.qualifier, Option(parent.metadata))
  }

  private def addCastsToArrayStructs(
      tableName: String,
      parent: NamedExpression,
      source: StructType,
      target: StructType,
      sourceNullable: Boolean): Expression = {
    val structConverter: (Expression, Expression) => Expression = (_, i) =>
      addCastsToStructs(tableName, Alias(GetArrayItem(parent, i), i.toString)(), source, target)
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
          val options = CaseInsensitiveMap(dataSourceV1.options)
          options.get(DeltaOptions.SCHEMA_TRACKING_LOCATION).foreach { rootSchemaTrackingLocation =>
            assert(options.get("path").isDefined, "Path for Delta table must be defined")
            val log = DeltaLog.forTable(session, options.get("path").get)
            val sourceIdOpt = options.get(DeltaOptions.STREAMING_SOURCE_TRACKING_ID)
            val schemaTrackingLocation = DeltaSourceSchemaTrackingLog.fullSchemaTrackingLocation(
              rootSchemaTrackingLocation, log.tableId, sourceIdOpt)
            // Make sure schema location is under checkpoint
            if (!allowSchemaLocationOutsideOfCheckpoint &&
              !(schemaTrackingLocation.stripSuffix("/") + "/")
                .startsWith(checkpointLocation.stripSuffix("/") + "/")) {
              throw DeltaErrors.schemaTrackingLocationNotUnderCheckpointLocation(
                schemaTrackingLocation, checkpointLocation)
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

  object EligibleCreateTableLikeCommand {
    def unapply(arg: LogicalPlan): Option[(CreateTableLikeCommand, CatalogTable)] = arg match {
      case c: CreateTableLikeCommand =>
        val src = session.sessionState.catalog.getTempViewOrPermanentTableMetadata(c.sourceTable)
        if (src.provider.contains("delta") || c.provider.exists(_.toLowerCase() == "delta")) {
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
        CDCReader.cdcReadSchema(d.schema()).toAttributes
      } else {
        v2Relation.output
      }
      val catalogTable = if (d.catalogTable.isDefined) {
        Some(d.v1Table)
      } else {
        None
      }
      LogicalRelation(relation, output, catalogTable, isStreaming = false)
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
      Dataset.ofRows(sparkSession, query)
    ).run(sparkSession)
  }
}

