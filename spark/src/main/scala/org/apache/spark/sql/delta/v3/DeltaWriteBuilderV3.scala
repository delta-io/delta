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

package org.apache.spark.sql.delta.v3

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsDynamicOverwrite, SupportsOverwrite, SupportsTruncate, Write, WriteBuilder}
import org.apache.spark.sql.delta.{ColumnWithDefaultExprUtils, DeltaErrors, DeltaOptions}
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.spark.sql.sources.Filter

/**
 * Delta v3 counterpart to [[org.apache.spark.sql.delta.catalog.WriteIntoDeltaBuilder]]. Same
 * `truncate` / `overwrite(filters)` / `overwriteDynamicPartitions` semantics, but `build()`
 * returns a [[DeltaFileWriteV3]] (a `FileWrite`) rather than a `V1Write` wrapping
 * `WriteIntoDelta`. The DSv2 plan stays DSv2 through analysis/optimization; the
 * [[DeltaFileWriteStrategy]] lowers it to a V2 exec node which calls back into the
 * [[DeltaFileOutputWriterV3]] on the driver.
 *
 * Carries over the `replaceOn` / `replaceUsing` validation and the `null-as-default` option
 * marker used by `TransactionalWrite.writeFiles`.
 */
class DeltaWriteBuilderV3(
    deltaTable: DeltaTableV3,
    info: LogicalWriteInfo,
    nullAsDefault: Boolean)
  extends WriteBuilder with SupportsOverwrite with SupportsTruncate with SupportsDynamicOverwrite {

  private val writeOptions = info.options()

  private val options =
    mutable.HashMap[String, String](writeOptions.asCaseSensitiveMap().asScala.toSeq: _*)

  private var forceOverwrite = false

  private def isReplaceOnOrUsingDefined: Boolean =
    writeOptions.containsKey(DeltaOptions.REPLACE_ON_OPTION) ||
      writeOptions.containsKey(DeltaOptions.REPLACE_USING_OPTION)

  override def truncate(): WriteBuilder = {
    forceOverwrite = true
    this
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    if (writeOptions.containsKey("replaceWhere")) {
      throw DeltaErrors.replaceWhereUsedInOverwrite()
    }
    if (isReplaceOnOrUsingDefined) {
      throw DeltaErrors.overwriteByFilterIncompatibleReplaceOnOrUsingError()
    }
    options.put("replaceWhere", DeltaSourceUtils.translateFilters(filters).sql)
    forceOverwrite = true
    this
  }

  override def overwriteDynamicPartitions(): WriteBuilder = {
    if (isReplaceOnOrUsingDefined) {
      throw DeltaErrors.dynamicPartitionOverwriteIncompatibleReplaceOnOrUsingError()
    }
    options.put(
      DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION,
      DeltaOptions.PARTITION_OVERWRITE_MODE_DYNAMIC)
    forceOverwrite = true
    this
  }

  override def build(): Write = {
    if (nullAsDefault) {
      options.put(ColumnWithDefaultExprUtils.USE_NULL_AS_DEFAULT_DELTA_OPTION, "true")
    }
    DeltaFileWriteV3(
      deltaTable = deltaTable,
      writeOptions = options.toMap,
      isOverwrite = forceOverwrite,
      isByName = false)
  }
}
