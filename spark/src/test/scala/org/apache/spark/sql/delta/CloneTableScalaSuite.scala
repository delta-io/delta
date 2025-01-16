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

class CloneTableScalaSuite extends CloneTableSuiteBase
  with DeltaColumnMappingTestUtils {

  // scalastyle:off argcount
  override protected def cloneTable(
      source: String,
      target: String,
      sourceIsTable: Boolean = false,
      targetIsTable: Boolean = false,
      targetLocation: Option[String] = None,
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None,
      isCreate: Boolean = true,
      isReplace: Boolean = false,
      tableProperties: Map[String, String] = Map.empty): Unit = {
    val table = if (sourceIsTable) {
      io.delta.tables.DeltaTable.forName(spark, source)
    } else {
      io.delta.tables.DeltaTable.forPath(spark, source)
    }

    if (versionAsOf.isDefined) {
      table.cloneAtVersion(versionAsOf.get, target, isReplace, tableProperties)
    } else if (timestampAsOf.isDefined) {
      table.cloneAtTimestamp(timestampAsOf.get, target, isReplace, tableProperties)
    } else {
      table.clone(target, isReplace, tableProperties)
    }
  }
  // scalastyle:on argcount
}
