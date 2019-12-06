package org.apache.spark.sql.delta

import java.net.URI

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.hive.DeltaStorageHandler
import org.apache.hadoop.fs._
import org.apache.hadoop.hive.metastore.api.{FieldSchema, MetaException}
import org.apache.hadoop.hive.ql.plan.TableScanDesc
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.actions.{AddFile, SingleAction}

object DeltaHelper extends Logging {

  def parsePathPartition(path: Path, partitionCols: Seq[String]): Map[String, String] = {
    val columns = ArrayBuffer.empty[(String, String)]
    // Old Hadoop versions don't have `Path.isRoot`
    var finished = path.getParent == null
    // currentPath is the current path that we will use to parse partition column value.
    var currentPath: Path = path

    while (!finished) {
      // Let's say currentPath is a path of "/table/a=1/", currentPath.getName will give us a=1.
      // Once we get the string, we try to parse it and find the partition column and value.
      val fragment = currentPath.getName
      val maybeColumn =
        parsePartitionColumn(currentPath.getName)

      maybeColumn.foreach(columns += _)

      finished =
        (maybeColumn.isEmpty && columns.nonEmpty) || currentPath.getParent == null

      if (!finished) {
        // For the above example, currentPath will be "/table/".
        currentPath = currentPath.getParent
      }
    }

    assert(columns.map(_._1).zip(partitionCols).forall(c => c._1 == c._2),
      s"""
         |partitionCols(${columns.map(_._1).mkString(",")}) parsed from $path
         |does not match the created table partition(${partitionCols.mkString(",")})
     """.stripMargin)

    columns.toMap
  }

  private def parsePartitionColumn(columnSpec: String): Option[(String, String)] = {
    import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.unescapePathName

    val equalSignIndex = columnSpec.indexOf('=')
    if (equalSignIndex == -1) {
      None
    } else {
      val columnName = unescapePathName(columnSpec.take(equalSignIndex))
      assert(columnName.nonEmpty, s"Empty partition column name in '$columnSpec'")

      val rawColumnValue = columnSpec.drop(equalSignIndex + 1)
      assert(rawColumnValue.nonEmpty, s"Empty partition column value in '$columnSpec'")

      Some(columnName, rawColumnValue)
    }
  }

  def listDeltaFiles(nonNormalizedPath: Path, job: JobConf): Array[FileStatus] = {
    val fs = nonNormalizedPath.getFileSystem(job)
    // We need to normalize the table path so that all paths we return to Hive will be normalized
    // This is necessary because `HiveInputFormat.pushProjectionsAndFilters` will try to figure out
    // which table a split path belongs to by comparing the split path with the normalized (? I have
    // not yet confirmed this) table paths.
    // TODO The assumption about Path in Hive is too strong, we should try to see if we can fail if
    // `pushProjectionsAndFilters` doesn't find a table for a Delta split path.
    val rootPath = fs.makeQualified(nonNormalizedPath)
    val deltaLog = DeltaLog.forTable(spark, rootPath)
    // get the snapshot of the version
    val snapshotToUse = deltaLog.snapshot

    // TODO Verify the table schema is consistent with `snapshotToUse.metadata`.

    // get the partition prune exprs
    val filterExprSerialized = job.get(TableScanDesc.FILTER_EXPR_CONF_STR)

    val convertedFilterExpr = DeltaPushFilter.partitionFilterConverter(filterExprSerialized)

    // The default value 128M is the same as the default value of
    // "spark.sql.files.maxPartitionBytes" in Spark. It's also the default parquet row group size
    // which is usually the best split size for parquet files.
    val blockSize = job.getLong("parquet.block.size", 128L * 1024 * 1024)

    // selected files to Hive to be processed
    DeltaLog.filterFileList(
      snapshotToUse.metadata.partitionColumns, snapshotToUse.allFiles.toDF(), convertedFilterExpr)
      .as[AddFile](SingleAction.addFileEncoder)
      .collect().map { f =>
        logInfo(s"selected delta file ${f.path} under $rootPath")
        toFileStatus(fs, rootPath, f, blockSize)
      }
  }

  /**
   * Convert an [[AddFile]] to Hadoop's [[FileStatus]].
   *
   * @param root the table path which will be used to create the real path from relative path.
   */
  private def toFileStatus(fs: FileSystem, root: Path, f: AddFile, blockSize: Long): FileStatus = {
    val status = new FileStatus(
      f.size, // length
      false, // isDir
      1, // blockReplication, FileInputFormat doesn't use this
      blockSize, // blockSize
      f.modificationTime, // modificationTime
      absolutePath(fs, root, f.path) // path
    )
    // We don't have `blockLocations` in `AddFile`. However, fetching them by calling
    // `getFileStatus` for each file is unacceptable because that's pretty inefficient and it will
    // make Delta look worse than a parquet table because of these FileSystem RPC calls.
    //
    // But if we don't set the block locations, [[FileInputFormat]] will try to fetch them. Hence,
    // we create a `LocatedFileStatus` with dummy block locations to save FileSystem RPC calls. We
    // lose the locality but this is fine today since most of storage systems are on Cloud and the
    // computation is running separately.
    //
    // An alternative solution is using "listStatus" recursively to get all `FileStatus`s and keep
    // those present in `AddFile`s. This is much cheaper and the performance should be the same as a
    // parquet table. However, it's pretty complicated as we need to be careful to avoid listing
    // unnecessary directories. So we decide to not do this right now.
    val dummyBlockLocations =
      Array(new BlockLocation(Array("localhost:50010"), Array("localhost"), 0, f.size))
    new LocatedFileStatus(status, dummyBlockLocations)
  }

  /**
   * Create an absolute [[Path]] from `child` using the `root` path if `child` is a relative path.
   * Return a [[Path]] version of child` if it is an absolute path.
   *
   * @param child an escaped string read from Delta's [[AddFile]] directly which requires to
   *              unescape before creating the [[Path]] object.
   */
  private def absolutePath(fs: FileSystem, root: Path, child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      fs.makeQualified(p)
    } else {
      new Path(root, p)
    }
  }

  def checkHiveColsInDelta(
      rootPath: Path,
      hiveSchema: java.util.List[FieldSchema]): Map[String, String] = {
    val deltaMeta = DeltaLog.forTable(spark, rootPath).snapshot.metadata
    assert(hiveSchema.size() == deltaMeta.schema.size,
      s"Hive cols(${hiveSchema.asScala.map(_.getName).mkString(",")}) number does not match " +
        s"Delta cols(${deltaMeta.schema.map(_.name).mkString(",")})")

    assert(hiveSchema.asScala.forall(h => deltaMeta.schema.exists(_.name == h.getName)),
      s"Hive cols(${hiveSchema.asScala.map(_.getName).mkString(",")}) name does not match " +
        s"Delta cols(${deltaMeta.schema.map(_.name).mkString(",")})")

    val (ds, ps) = hiveSchema.asScala.splitAt(hiveSchema.size() - deltaMeta.partitionColumns.size)

    if (ds.forall(s => deltaMeta.dataSchema.exists(_.name == s.getName))
      && ps.forall(s => deltaMeta.partitionColumns.contains(s.getName))) {
      Map(DeltaStorageHandler.DELTA_PARTITION_COLS_NAMES -> ps.map(_.getName).mkString(","),
        DeltaStorageHandler.DELTA_PARTITION_COLS_TYPES -> ps.map(_.getType).mkString(":"))
    } else {
      throw new MetaException(s"The partition cols of Delta should be after data cols " +
        s"when creating hive table. Delta dataschema is " +
        s"${deltaMeta.dataSchema.json} and partitionschema is ${deltaMeta.partitionSchema.json}")
    }
  }

  def getPartitionCols(rootPath: Path): Seq[String] = {
    DeltaLog.forTable(spark, rootPath).snapshot.metadata.partitionColumns
  }

  def spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("HiveOnDelta Get Files")
    .getOrCreate()
}