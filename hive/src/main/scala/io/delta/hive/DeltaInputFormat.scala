package io.delta.hive

import java.io.IOException

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
import org.apache.hadoop.mapred.FileInputFormat._
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.actions.{AddFile, SingleAction}
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.delta.{DeltaHelper, DeltaLog, DeltaTableUtils}

class DeltaInputFormat extends MapredParquetInputFormat {

  import DeltaInputFormat._

  @throws[IOException]
  override def listStatus(job: JobConf): Array[FileStatus] = try {
    val dirs = getInputPaths(job)
    if (dirs.isEmpty) {
      throw new IOException("No input paths specified in job")
    } else {
      TokenCache.obtainTokensForNamenodes(job.getCredentials, dirs, job)

      // find delta root path
      val rootPath = DeltaTableUtils.findDeltaTableRoot(spark, dirs.head).get
      val deltaLog = DeltaLog.forTable(spark, rootPath)
      // get the snapshot of the version
      val snapshotToUse = deltaLog.snapshot

      val fs = rootPath.getFileSystem(job)

      // get partition filters
      val partitionFilters = if (rootPath != dirs.head) {
        val partitionFragments = dirs.map { dir =>
          val relativePath = DeltaFileOperations.tryRelativizePath(fs, rootPath, dir)
          assert(
            !relativePath.isAbsolute,
            s"Fail to relativize path $dir against base path $rootPath.")
          relativePath.toUri.toString
        }
        DeltaHelper.resolvePathFilters(snapshotToUse, partitionFragments)
      } else {
        assert(dirs.length == 1, "Not-partitioned table should only have one input dir.")
        Nil
      }

      // selected files to Hive to be processed
      DeltaLog.filterFileList(
        snapshotToUse.metadata.partitionColumns, snapshotToUse.allFiles.toDF(), partitionFilters)
        .as[AddFile](SingleAction.addFileEncoder)
        .collect()
        .map { file =>
          fs.getFileStatus(new Path(rootPath, file.path))
        }
    }
  } catch {
    case e: Throwable =>
      e.printStackTrace()
      throw e
  }
}

object DeltaInputFormat {
  def spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("HiveOnDelta Get Files")
    .getOrCreate()
}
