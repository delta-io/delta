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

package io.delta.sql


import io.delta.kernel.spark.catalog.SparkTable
import org.apache.spark.sql.delta.DeltaTableUtils
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, CloneTableStatement, InsertIntoStatement, LogicalPlan, MergeIntoTable, OverwriteByExpression, OverwritePartitionsDynamic, RestoreTableStatement}
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeInto
import org.apache.spark.sql.catalyst.TimeTravel
import org.apache.spark.sql.catalyst.streaming.{WriteToStream, WriteToStreamStatement}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.delta.commands.{DeltaCommand, DescribeDeltaDetailCommand}
import org.apache.spark.sql.delta.TableChanges
import org.apache.spark.sql.execution.datasources.{DataSource, DataSourceUtils}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MaybeFallbackV1Connector(session: SparkSession)
  extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    def replaceKernelWithFallback(node: LogicalPlan): LogicalPlan = {
      node.resolveOperatorsDown {
        case Batch(fallback) => fallback
        case Streaming(fallback) => fallback
        case ResolvedTableWithSparkTable(fallback) => fallback
      }
    }
    plan.resolveOperatorsDown {
      // Handle ResolvedTable with SparkTable (for commands like DESCRIBE HISTORY)
      case ResolvedTableWithSparkTable(fallback) =>
        fallback
      // Handle MERGE INTO (Spark generic MergeIntoTable)
      case m @ MergeIntoTable(targetTable, sourceTable, mergeCondition,
        matchedActions, notMatchedActions, notMatchedBySourceActions) =>
        val newTarget = replaceKernelWithFallback(targetTable)
        val newSource = replaceKernelWithFallback(sourceTable)
        m.copy(targetTable = newTarget, sourceTable = newSource)

      // Handle MERGE INTO (DeltaMergeInto)
      case m @ DeltaMergeInto(target, source, condition, matched, notMatched, notMatchedBySource,
        withSchemaEvolution, finalSchema) =>
        val newTarget = replaceKernelWithFallback(target)
        val newSource = replaceKernelWithFallback(source)
        m.copy(target = newTarget, source = newSource)

      // Handle V1 INSERT INTO
      case i @ InsertIntoStatement(table, part, cols, query, overwrite, byName, ifNotExists) =>
        val newTable = replaceKernelWithFallback(table)
        i.copy(table = newTable)

      // Handle CLONE TABLE (both source and target need fallback)
      case c @ CloneTableStatement(source, target, ifNotExists, isReplace, isCreate,
        tablePropertyOverrides, targetLocation) =>
        val newSource = replaceKernelWithFallback(source)
        val newTarget = replaceKernelWithFallback(target)
        c.copy(source = newSource, target = newTarget)

      // Handle table_changes() table-valued function
      case tc @ TableChanges(child, fnName, cdcAttr) =>
        val newChild = replaceKernelWithFallback(child)
        tc.copy(child = newChild)

      // Handle DESCRIBE DETAIL command
      case dd @ DescribeDeltaDetailCommand(child, hadoopConf) =>
        val newChild = replaceKernelWithFallback(child)
        dd.copy(child = newChild)

      // Handle RESTORE TABLE command
      case restore @ RestoreTableStatement(timeTravel) =>
        // TimeTravel contains a relation that needs fallback
        val newTimeTravel = timeTravel.transformUp {
          case Batch(fallback) => fallback
          case ResolvedTableWithSparkTable(fallback) => fallback
        }
        restore.copy(table = newTimeTravel.asInstanceOf[TimeTravel])

      // Handle WriteToStream - need to fallback both sink and source
      case w @ WriteToStream(name, checkpoint, sink, outputMode, deleteCheckpoint,
        inputQuery, catalogAndIdent, catalogTable) =>
        // scalastyle:off println
        println(s"[MaybeFallbackV1] WriteToStream - sink: ${sink.getClass.getSimpleName}")
        println(s"[MaybeFallbackV1] WriteToStream - inputQuery: " +
          s"${inputQuery.getClass.getSimpleName}")
        // scalastyle:on println
        // Process the input query to replace any SparkTable streaming sources
        val newTable = TableFallback.unapply(sink)
        // scalastyle:off println
        println(s"[MaybeFallbackV1] WriteToStream - new table: " +
          s"${newTable.getClass.getSimpleName}")
        // scalastyle:on println
        if(newTable.isDefined) {
          w.copy(sink = newTable.get)
        }
        w

      // Handle V2 AppendData (DataFrameWriterV2.append)
      case a @ AppendData(Batch(fallback), _, _, _, _, _) =>
        a.copy(table = fallback)

      // Handle V2 OverwriteByExpression (DataFrameWriterV2.overwrite)
      case o @ OverwriteByExpression(Batch(fallback), _, _, _, _, _, _) =>
        o.copy(table = fallback)

      // Handle V2 OverwritePartitionsDynamic (DataFrameWriterV2.overwritePartitions)
      case o @ OverwritePartitionsDynamic(Batch(fallback), _, _, _, _) =>
        o.copy(table = fallback)

      // Handle CDF reads - must fallback even for read-only queries
      // because Kernel doesn't expose CDC metadata columns (_change_type, _commit_version, etc.)
      case dsv2: DataSourceV2Relation if isCDFRead(dsv2) =>
        // scalastyle:off println
        println(s"[MaybeFallbackV1] CDF read detected, forcing fallback to V1")
        // scalastyle:on println
        Batch.unapply(dsv2).getOrElse(dsv2)

      // Handle batch reads
      case Batch(fallback) if !isReadOnly(plan) =>
        fallback

      // Handle streaming
      case Streaming(fallback) if !isReadOnly(plan) =>
        fallback

      // Print unhandled COMMAND nodes
      case other if other.containsPattern(COMMAND) =>
        other
    }
  }

  private def isReadOnly(plan: LogicalPlan): Boolean = {
    !plan.containsPattern(COMMAND) && !plan.exists(_.isInstanceOf[InsertIntoStatement]) &&
      !plan.exists(_.isInstanceOf[DeltaCommand])
  }

  /**
   * Check if a DataSourceV2Relation is a CDF (Change Data Feed) read query.
   * CDF queries need to fallback to V1 because Kernel doesn't expose CDC metadata columns.
   */
  private def isCDFRead(dsv2: DataSourceV2Relation): Boolean = {
    import org.apache.spark.sql.delta.sources.DeltaDataSource
    val options = dsv2.options
    (options.containsKey(DeltaDataSource.CDC_ENABLED_KEY) &&
      options.get(DeltaDataSource.CDC_ENABLED_KEY).equalsIgnoreCase("true")) ||
    (options.containsKey(DeltaDataSource.CDC_ENABLED_KEY_LEGACY) &&
      options.get(DeltaDataSource.CDC_ENABLED_KEY_LEGACY).equalsIgnoreCase("true"))
  }

  object Batch {
    def unapply(dsv2: DataSourceV2Relation): Option[DataSourceV2Relation] = dsv2.table match {
      case d: SparkTable =>
        val v1CatalogTable = d.getV1CatalogTable()
        if (v1CatalogTable.isPresent()) {
          val catalogTable = v1CatalogTable.get()
          Some(dsv2.copy(table = DeltaTableV2(
            session,
            new Path(catalogTable.location),
            catalogTable = Some(catalogTable),
            tableIdentifier = Some(catalogTable.identifier.toString))))
        } else {
          Some(dsv2.copy(table = DeltaTableV2(session, new Path(d.name()))))
        }
      case _ => None
    }
  }
  object Streaming {
    def unapply(dsv2: StreamingRelationV2): Option[StreamingRelation] = dsv2.table match {
      case d: SparkTable =>
        // Streaming's fallback is not via DeltaAnalysis, so directly create v1 streaming relation.
        val v1CatalogTable = d.getV1CatalogTable()
        assert(v1CatalogTable.isPresent())
        val catalogTable = v1CatalogTable.get()
        Some(getStreamingRelation(catalogTable, dsv2.extraOptions))
      case _ => None
    }
  }

  object TableFallback {
    def unapply(table: Table): Option[Table] = table match {
      case s: SparkTable =>
        val v1CatalogTable = s.getV1CatalogTable()
        if (v1CatalogTable.isPresent()) {
          val catalogTable = v1CatalogTable.get()
          val deltaTableV2 = DeltaTableV2(
            session,
            new Path(catalogTable.location),
            catalogTable = Some(catalogTable),
            tableIdentifier = Some(catalogTable.identifier.toString))
          Some(deltaTableV2)
        } else {
          val deltaTableV2 = DeltaTableV2(session, new Path(s.name()))
          Some(deltaTableV2)
        }
      case _ => None
    }
  }

  object ResolvedTableWithSparkTable {
    def unapply(resolved: ResolvedTable): Option[ResolvedTable] = resolved.table match {
      case s: SparkTable =>
        val v1CatalogTable = s.getV1CatalogTable()
        if (v1CatalogTable.isPresent()) {
          val catalogTable = v1CatalogTable.get()
          val deltaTableV2 = DeltaTableV2(
            session,
            new Path(catalogTable.location),
            catalogTable = Some(catalogTable),
            tableIdentifier = Some(catalogTable.identifier.toString))
          Some(resolved.copy(table = deltaTableV2))
        } else {
          val deltaTableV2 = DeltaTableV2(session, new Path(s.name()))
          Some(resolved.copy(table = deltaTableV2))
        }
      case _ => None
    }
  }

  private def getStreamingRelation(
                                    table: CatalogTable,
                                    extraOptions: CaseInsensitiveStringMap): StreamingRelation = {
    val dsOptions = DataSourceUtils.generateDatasourceOptions(extraOptions, table)
    val dataSource = DataSource(
      SparkSession.active,
      className = table.provider.get,
      userSpecifiedSchema = if (!DeltaTableUtils.isDeltaTable(table)) {
        Some(table.schema)
      } else None,
      options = dsOptions,
      catalogTable = Some(table))
    StreamingRelation(dataSource)
  }
}
