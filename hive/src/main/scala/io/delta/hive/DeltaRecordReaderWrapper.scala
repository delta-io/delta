/*
 * Copyright 2019 Databricks, Inc.
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

import org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.Reporter
import org.apache.parquet.hadoop.ParquetInputFormat
import org.slf4j.LoggerFactory

/**
 * A record reader that reads data from the underlying Parquet reader and inserts partition values
 * which don't exist in the Parquet files.
 *
 * As we have verified the Hive schema in metastore is consistent with the Delta schema, the row
 * returned by the underlying Parquet reader will match the Delta schema except that it leaves all
 * partition columns as `null` since they are not in the raw parquet files. Hence, for the missing
 * partition values, we need to use the partition information in [[DeltaInputSplit]] to create the
 * corresponding [[Writable]]s, and insert them into the corresponding positions when reading a row.
 */
class DeltaRecordReaderWrapper(
    inputFormat: ParquetInputFormat[ArrayWritable],
    split: DeltaInputSplit,
    jobConf: JobConf,
    reporter: Reporter) extends ParquetRecordReaderWrapper(inputFormat, split, jobConf, reporter) {

  private val LOG = LoggerFactory.getLogger(classOf[DeltaRecordReaderWrapper])

  /** The positions of partition columns in Delta schema and their corresponding values. */
  private val partitionValues: Array[(Int, Writable)] =
    split.getPartitionColumns.map { partition =>
      val oi = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableObjectInspector(TypeInfoFactory
          .getPrimitiveTypeInfo(partition.tpe))
      val partitionValue = ObjectInspectorConverters.getConverter(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        oi).convert(partition.value).asInstanceOf[Writable]
      (partition.index, partitionValue)
    }

  override def next(key: NullWritable, value: ArrayWritable): Boolean = {
    val hasNext = super.next(key, value)
    // TODO Figure out when the parent reader resets partition columns to null so that we may come
    // out a better solution to not insert partition values for each row.
    if (hasNext) {
      insertPartitionValues(value)
    }
    hasNext
  }

  /**
   * As partition columns are not in the parquet files, they will be set to `null`s every time
   * `next` is called. We should insert partition values manually for each row.
   */
  private def insertPartitionValues(value: ArrayWritable): Unit = {
    val valueArray = value.get()
    var i = 0
    val n = partitionValues.length
    // Using while loop for better performance since this method is called for each row.
    while (i < n) {
      val partition = partitionValues(i)
      // The schema of `valueArray` is the Hive schema, and it's the same as the Delta
      // schema since we have verified it in `DeltaInputFormat`. Hence, the position of a partition
      // column in `valueArray` is the same as its position in Delta schema.
      valueArray(partition._1) = partition._2
      i += 1
    }
  }
}
