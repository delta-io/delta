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
