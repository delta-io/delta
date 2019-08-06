package io.delta.hive

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
import org.apache.hadoop.mapred.FileInputFormat._
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.security.TokenCache
import java.io.IOException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.actions.SingleAction._
import org.apache.spark.sql.delta.sources.DeltaDataSource
import org.apache.spark.sql.delta.{DeltaLog, DeltaTableUtils}

class DeltaInputFormat extends MapredParquetInputFormat with Logging {
  import DeltaInputFormat._

  @throws[IOException]
  override def listStatus(job: JobConf): Array[FileStatus] = {
    val dirs = getInputPaths(job)
    if (dirs.isEmpty) {
      throw new IOException("No input paths specified in job")
    } else {
      TokenCache.obtainTokensForNamenodes(job.getCredentials, dirs, job)

      // find delta root path
      val rootPath = DeltaTableUtils.findDeltaTableRoot(spark, dirs)

      // determine the version to use
      val parameters = Map(
        DeltaDataSource.TIME_TRAVEL_TIMESTAMP_KEY ->
          job.get(DeltaDataSource.TIME_TRAVEL_TIMESTAMP_KEY),
        DeltaDataSource.TIME_TRAVEL_VERSION_KEY ->
          job.get(DeltaDataSource.TIME_TRAVEL_VERSION_KEY),
        DeltaDataSource.TIME_TRAVEL_SOURCE_KEY ->
          job.get(DeltaDataSource.TIME_TRAVEL_SOURCE_KEY)
      ).filter(_._2 != null)

      val (realPath, timeTravelOpt) =
        DeltaTableUtils.getTimeTravel(spark, rootPath.toString, parameters)
      val deltaLog = DeltaLog.forTable(spark, realPath)
      val versionToUse = timeTravelOpt.map { tt =>
        val (version, accessType) = DeltaTableUtils.resolveTimeTravelVersion(
          spark.sessionState.conf, deltaLog, tt)
        val source = tt.creationSource.getOrElse("unknown")
        version
      }
      // get the snapshot of the version
      val snapshotToUse = versionToUse.map(deltaLog.getSnapshotAt(_)).getOrElse(deltaLog.snapshot)

      // get partition filters
      val partitionFragments = dirs.map(_.toString.substring(rootPath.toString.length() + 1))
      val partitionFilters = if (rootPath != dirs(0)) {
        DeltaTableUtils.resolvePathFilters(deltaLog, partitionFragments)
      } else {
        assert(dirs.length == 1, "Not-partitioned table should only have one input dir.")
        Nil
      }

      // selected files to Hive to be processed
      val fs: FileSystem = rootPath.getFileSystem(job)
      DeltaLog.filterFileList(
        snapshotToUse.metadata.partitionColumns, snapshotToUse.allFiles.toDF(), partitionFilters)
        .as[AddFile]
        .collect()
        .map { file =>
          logDebug(s"delta file:$file is selected")
          fs.getFileStatus(new Path(rootPath, file.path))
        }
    }
  }
}

object DeltaInputFormat {
  lazy val spark = SparkSession.builder()
    .master("local[*]")
    .appName("HiveOnDelta Get Files")
    .getOrCreate()
}