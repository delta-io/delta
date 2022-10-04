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

package org.apache.spark.sql.delta.test

import java.io.File

import org.apache.spark.sql.delta.{DeltaLog, DeltaLogObjectBase, DeltaTableIdentifier, OptimisticTransaction}
import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions.{Action, Metadata}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.util.Clock

/**
 * Additional method definitions for Delta classes that are intended for use only in testing.
 */
object DeltaTestImplicits {
  implicit class OptimisticTxnTestHelper(txn: OptimisticTransaction) {
    /** Ensures that the initial commit of a Delta table always contains a Metadata action */
    def commitManually(actions: Action*): Long = {
      if (txn.readVersion == -1 && !actions.exists(_.isInstanceOf[Metadata])) {
        txn.commit(Metadata() +: actions, ManualUpdate)
      } else {
        txn.commit(actions, ManualUpdate)
      }
    }
  }

  implicit class DeltaLogForTableOverloads(self: DeltaLogObjectBase) {
    import DeltaLog.{apply, logPathFor}

    /** Creates a DeltaLog from a path string, with options */
    def forTable(spark: SparkSession, dataPath: String, options: Map[String, String]): DeltaLog = {
      apply(spark, logPathFor(dataPath), options = options)
    }

    /** Creates a DeltaLog from [[File]] */
    def forTable(spark: SparkSession, dataPath: File): DeltaLog = {
      apply(spark, logPathFor(dataPath.getAbsolutePath))
    }

    /** Creates a DeltaLog from [[File]], with a custom clock */
    def forTable(spark: SparkSession, dataPath: File, clock: Clock): DeltaLog = {
      apply(spark, logPathFor(dataPath.getAbsolutePath), clock = Some(clock))
    }

    /** Creates a DeltaLog from a string path, with a custom clock */
    def forTable(spark: SparkSession, dataPath: String, clock: Clock): DeltaLog = {
      apply(spark, logPathFor(dataPath), clock = Some(clock))
    }

    /** Creates a DeltaLog with a custom clock */
    def forTable(spark: SparkSession, dataPath: Path, clock: Clock): DeltaLog = {
      apply(spark, logPathFor(dataPath), clock = Some(clock))
    }

    /** Creates a DeltaLog from a [[TableIdentifier]], with a custom clock */
    def forTable(spark: SparkSession, tableName: TableIdentifier, clock: Clock): DeltaLog = {
      if (DeltaTableIdentifier.isDeltaPath(spark, tableName)) {
        forTable(spark, tableName.table, clock)
      } else {
        forTable(spark, spark.sessionState.catalog.getTableMetadata(tableName), clock)
      }
    }

    /** Creates a DeltaLog from a [[CatalogTable]], with a custom clock */
    def forTable(spark: SparkSession, table: CatalogTable, clock: Clock): DeltaLog = {
      apply(spark, logPathFor(new Path(table.location)), clock = Some(clock))
    }
  }
}
