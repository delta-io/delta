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

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.Utils

/** Restore tests using the Scala APIs. */
class RestoreTableScalaSuite extends RestoreTableSuiteBase {

  override def restoreTableToVersion(
      tblId: String,
      version: Int,
      isTable: Boolean,
      expectNoOp: Boolean = false): DataFrame = {
    val deltaTable = if (isTable) {
      io.delta.tables.DeltaTable.forName(spark, tblId)
    } else {
      io.delta.tables.DeltaTable.forPath(spark, tblId)
    }

    deltaTable.restoreToVersion(version)
  }

  override def restoreTableToTimestamp(
      tblId: String,
      timestamp: String,
      isTable: Boolean,
      expectNoOp: Boolean = false): DataFrame = {
    val deltaTable = if (isTable) {
      io.delta.tables.DeltaTable.forName(spark, tblId)
    } else {
      io.delta.tables.DeltaTable.forPath(spark, tblId)
    }

    deltaTable.restoreToTimestamp(timestamp)
  }
}

