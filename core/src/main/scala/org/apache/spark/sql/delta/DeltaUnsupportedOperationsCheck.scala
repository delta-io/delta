/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSourceUtils

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, DropTable, LogicalPlan, OverwriteByExpression, V2WriteCommand}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * A rule to add helpful error messages when Delta is being used with unsupported Hive operations
 * or if an unsupported operation is being made, e.g. a DML operation like
 * INSERT/UPDATE/DELETE/MERGE when a table doesn't exist.
 */
case class DeltaUnsupportedOperationsCheck(spark: SparkSession)
  extends (LogicalPlan => Unit)
  with DeltaLogging {

  private def fail(operation: String, tableIdent: TableIdentifier): Unit = {
    if (DeltaTableUtils.isDeltaTable(spark, tableIdent)) {
      throw DeltaErrors.operationNotSupportedException(operation, tableIdent)
    }
  }

  private def fail(operation: String, provider: String): Unit = {
    if (DeltaSourceUtils.isDeltaDataSourceName(provider)) {
      throw DeltaErrors.operationNotSupportedException(operation)
    }
  }

  def apply(plan: LogicalPlan): Unit = plan.foreach {
    // Unsupported Hive commands
    case c: CreateTableLikeCommand =>
      recordDeltaEvent(null, "delta.unsupported.createLike")
      fail(operation = "CREATE TABLE LIKE", c.sourceTable)

      def targetTableProvider: String = {
        val catalog = spark.sessionState.catalog
        val sourceTableDesc = catalog.getTempViewOrPermanentTableMetadata(c.sourceTable)
        if (c.provider.isDefined) {
          c.provider.get
        } else if (sourceTableDesc.tableType == CatalogTableType.VIEW) {
          spark.sessionState.conf.defaultDataSourceName
        } else if (c.fileFormat.inputFormat.isDefined) {
          DDLUtils.HIVE_PROVIDER
        } else {
          sourceTableDesc.provider.getOrElse(spark.sessionState.conf.defaultDataSourceName)
        }
      }

      fail(operation = "CREATE TABLE LIKE", targetTableProvider)

    case a: AnalyzePartitionCommand =>
      recordDeltaEvent(null, "delta.unsupported.analyzePartition")
      fail(operation = "ANALYZE TABLE PARTITION", a.tableIdent)

    case a: AlterTableAddPartitionCommand =>
      recordDeltaEvent(null, "delta.unsupported.addPartition")
      fail(operation = "ALTER TABLE ADD PARTITION", a.tableName)

    case a: AlterTableDropPartitionCommand =>
      recordDeltaEvent(null, "delta.unsupported.dropPartition")
      fail(operation = "ALTER TABLE DROP PARTITION", a.tableName)

    case a: AlterTableRecoverPartitionsCommand =>
      recordDeltaEvent(null, "delta.unsupported.recoverPartitions")
      fail(operation = "ALTER TABLE RECOVER PARTITIONS", a.tableName)

    case a: AlterTableSerDePropertiesCommand =>
      recordDeltaEvent(null, "delta.unsupported.alterSerDe")
      fail(operation = "ALTER TABLE table SET SERDEPROPERTIES", a.tableName)

    case l: LoadDataCommand =>
      recordDeltaEvent(null, "delta.unsupported.loadData")
      fail(operation = "LOAD DATA", l.table)

    case i: InsertIntoDataSourceDirCommand =>
      recordDeltaEvent(null, "delta.unsupported.insertDirectory")
      fail(operation = "INSERT OVERWRITE DIRECTORY", i.provider)

    // Delta table checks
    case append: AppendData =>
      val op = if (append.isByName) "APPEND" else "INSERT"
      checkDeltaTableExists(append, op)

    case overwrite: OverwriteByExpression =>
      checkDeltaTableExists(overwrite, "OVERWRITE")

    case _: DropTable =>
      // For Delta tables being dropped, we do not need the underlying Delta log to exist so this is
      // OK
      return

    case DataSourceV2Relation(tbl: DeltaTableV2, _, _, _, _) if !tbl.deltaLog.tableExists =>
      throw DeltaErrors.pathNotExistsException(tbl.deltaLog.dataPath.toString)

    case r: ResolvedTable if r.table.isInstanceOf[DeltaTableV2] &&
        !r.table.asInstanceOf[DeltaTableV2].deltaLog.tableExists =>
      throw DeltaErrors.pathNotExistsException(
        r.table.asInstanceOf[DeltaTableV2].deltaLog.dataPath.toString)

    case _ => // OK
  }

  /**
   * Check that the given operation is being made on a full Delta table that exists.
   */
  private def checkDeltaTableExists(command: V2WriteCommand, operation: String): Unit = {
    command.table match {
      case DeltaRelation(lr) =>
        // the extractor performs the check that we want if this is indeed being called on a Delta
        // table. It should leave others unchanged
        if (DeltaFullTable.unapply(lr).isEmpty) {
          throw DeltaErrors.notADeltaTableException(operation)
        }
      case _ =>
    }
  }
}
