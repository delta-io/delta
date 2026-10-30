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


package org.apache.spark.sql.delta.amt

import org.apache.spark.sql.delta.RowIndexFilterProvider
import org.apache.spark.sql.delta.DeltaLogFileIndex
import org.apache.hadoop.fs.FileStatus

/** A file index used to read AMT leaf manifests. */
private[amt] final class AMTLeafFileIndex(
    format: AMTParquetFileFormat,
    files: Array[FileStatus],
    perFileRowIndexFilters: Map[String, RowIndexFilterProvider])
  extends DeltaLogFileIndex(format, files, perFileRowIndexFilters)
