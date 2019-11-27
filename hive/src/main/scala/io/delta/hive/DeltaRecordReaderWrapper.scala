package io.delta.hive

import com.google.common.base.Joiner
import scala.collection.JavaConverters._

import org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.Reporter
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.sql.delta.DeltaHelper
import org.slf4j.LoggerFactory

class DeltaRecordReaderWrapper(newInputFormat: ParquetInputFormat[ArrayWritable], oldSplit: InputSplit, oldJobConf: JobConf, reporter: Reporter) extends ParquetRecordReaderWrapper(newInputFormat, oldSplit, oldJobConf, reporter) {

  private val LOG = LoggerFactory.getLogger(classOf[DeltaRecordReaderWrapper])

  private val partitionWritable: Array[Writable] =
    if (!oldSplit.isInstanceOf[FileSplit]) {
      throw new IllegalArgumentException("Unknown split type: " + oldSplit)
    } else {
      val columnNameProperty = oldJobConf.get(DeltaStorageHandler.DELTA_PARTITION_COLS_NAMES)
      val columnTypeProperty = oldJobConf.get(DeltaStorageHandler.DELTA_PARTITION_COLS_TYPES)
      LOG.info("Delta partition cols: " + columnNameProperty + " with types: " + columnTypeProperty)

      if (columnNameProperty == null || columnNameProperty.trim().length() == 0
        || columnTypeProperty == null || columnTypeProperty.trim().length() == 0) {
        LOG.info("No partition info is provided...")
        null
      } else {
        // generate partition writale values which will be appended after data values from parquet
        val columnNames = columnNameProperty.split(",")
        val columnTypes = columnTypeProperty.split(":")

        val filePath = oldSplit.asInstanceOf[FileSplit].getPath()
        val parsedPartitions = DeltaHelper.parsePathPartition(filePath, columnNames).asJava

        val partitionWritable = new Array[Writable](columnNames.length)
        // inspect partition values
        for (i <- 0 until columnNames.length) {
          val oi = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(TypeInfoFactory
              .getPrimitiveTypeInfo(columnTypes(i)))

          partitionWritable(i) = ObjectInspectorConverters.getConverter(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, oi).convert(parsedPartitions.get(columnNames(i))).asInstanceOf[Writable]
        }
        LOG.info("Parsed partition values from " + filePath.toString() + " list: " + Joiner.on(",").withKeyValueSeparator("=").join(parsedPartitions)
          + ", partitionWritable length:" + partitionWritable.length)
        partitionWritable
      }
    }

  override def next(key: NullWritable, value: ArrayWritable): Boolean = {
    val hasNext = super.next(key, value)
    if (partitionWritable != null && partitionWritable.length != 0) {
      // append partition values to data values
      for (i <- 0 until partitionWritable.length) {
        value.get()(value.get().length - partitionWritable.length + i) = partitionWritable(i)
      }
    }
    hasNext
  }

  override def createValue(): ArrayWritable = {
    val value = super.createValue()
    if (partitionWritable != null && partitionWritable.length != 0) {
      for (i <- 0 until partitionWritable.length) {
        value.get()(value.get().length - partitionWritable.length + i) = partitionWritable(i)
      }
    }
    value
  }
}
