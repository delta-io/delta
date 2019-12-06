package io.delta.hive

import java.io.IOException

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.sql.delta.DeltaHelper
import org.slf4j.LoggerFactory

class DeltaInputFormat(realInput: ParquetInputFormat[ArrayWritable]) extends FileInputFormat[NullWritable, ArrayWritable] {

  private val LOG = LoggerFactory.getLogger(classOf[DeltaInputFormat])

  def this() {
    this(new ParquetInputFormat[ArrayWritable](classOf[DataWritableReadSupport]))
  }

  override def getRecordReader(split: InputSplit, job: JobConf, reporter: Reporter): RecordReader[NullWritable, ArrayWritable] = {
    if (Utilities.getUseVectorizedInputFileFormat(job)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using vectorized record reader")
      }
      throw new IOException("Currently not support Delta VectorizedReader")
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using row-mode record reader")
      }
      new DeltaRecordReaderWrapper(this.realInput, split, job, reporter)
    }
  }

  override def listStatus(job: JobConf): Array[FileStatus] = {
    val deltaRootPath = new Path(job.get(DeltaStorageHandler.DELTA_TABLE_PATH))
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), Array(deltaRootPath), job)
    DeltaHelper.listDeltaFiles(deltaRootPath, job)
  }
}
