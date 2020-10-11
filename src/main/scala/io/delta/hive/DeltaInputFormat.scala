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

import org.apache.hadoop.io.{ArrayWritable, NullWritable}
import org.apache.hadoop.mapred.{FileInputFormat, InputSplit, JobConf, RecordReader, Reporter}
import org.apache.parquet.hadoop.ParquetInputFormat

class DeltaInputFormat(realInput: ParquetInputFormat[ArrayWritable])
  extends FileInputFormat[NullWritable, ArrayWritable] {

  override def getRecordReader(split: InputSplit, job: JobConf, reporter: Reporter):
  RecordReader[NullWritable, ArrayWritable] = throw new UnsupportedOperationException(
    "This are the mock class in order to be able spark to run")
}
