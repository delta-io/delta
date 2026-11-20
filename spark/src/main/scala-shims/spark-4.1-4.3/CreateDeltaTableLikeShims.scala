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

package org.apache.spark.sql.delta.commands

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.delta.DeltaOptions

object CreateDeltaTableLikeShims {

  /**
   * Differentiate between DataFrameWriterV1 and V2 so that we can decide
   * what to do with table metadata. In DataFrameWriterV1, mode("overwrite").saveAsTable,
   * behaves as a CreateOrReplace table, but we have asked for "overwriteSchema" as an
   * explicit option to overwrite partitioning or schema information. With DataFrameWriterV2,
   * the behavior asked for by the user is clearer: .createOrReplace(), which means that we
   * should overwrite schema and/or partitioning. Therefore we have this hack.
   *
   * DataFrameWriter provides the option "__v1_save_as_table_overwrite", because the stack trace
   * does not indicate the calling API anymore in connect mode - planning and execution has been
   * separated.
   *
   * TODO: this shim is now the same for every supported Spark version and can be inlined.
   */
  def isV1WriterSaveAsTableOverwrite(options: DeltaOptions, mode: SaveMode): Boolean = {
    // Note: Spark is setting this only for SaveMode.Overwrite anyway, but we double check.
    options.isDataFrameWriterV1SaveAsTableOverwrite && mode == SaveMode.Overwrite
  }
}
