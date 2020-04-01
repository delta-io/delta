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

package io.delta.hive

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.{ArrayWritable, NullWritable}
import org.apache.hadoop.mapred.{JobConf, OutputFormat, RecordWriter}
import org.apache.hadoop.util.Progressable

/**
 * This class is not a real implementation. We use it to prevent from writing to a Delta table in
 * Hive before we support it.
 */
class DeltaOutputFormat extends OutputFormat[NullWritable, ArrayWritable] {

  private def writingNotSupported[T](): T = {
    throw new UnsupportedOperationException(
      "Writing to a Delta table in Hive is not supported. Please use Spark to write.")
  }

  override def getRecordWriter(
    ignored: FileSystem,
    job: JobConf,
    name: String,
    progress: Progressable): RecordWriter[NullWritable, ArrayWritable] = writingNotSupported()

  override def checkOutputSpecs(ignored: FileSystem, job: JobConf): Unit = writingNotSupported()
}
