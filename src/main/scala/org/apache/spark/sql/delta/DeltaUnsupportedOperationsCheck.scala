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

import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSourceUtils

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.RefreshTable

/**
 * A rule to check whether the functions are supported only when Hive support is enabled
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

  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case c: CreateTableLikeCommand =>
        recordDeltaEvent(null, "delta.unsupported.createLike")
        fail(operation = "CREATE TABLE LIKE", c.sourceTable)

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

      case _ => // OK
    }
  }
}
