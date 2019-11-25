package org.apache.spark.sql.delta

import io.delta.hive.DeltaStorageHandler
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{FileStatus, Path}
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

  def listDeltaFiles(rootPath: Path, job: JobConf): java.util.List[FileStatus] = {
    val deltaLog = DeltaLog.forTable(spark, rootPath)
    // get the snapshot of the version
    val snapshotToUse = deltaLog.snapshot

    val fs = rootPath.getFileSystem(job)

    // get the partition prune exprs
    val filterExprSerialized = job.get(TableScanDesc.FILTER_EXPR_CONF_STR)

    val convertedFilterExpr = DeltaPushFilter.partitionFilterConverter(filterExprSerialized)

    // selected files to Hive to be processed
    DeltaLog.filterFileList(
      snapshotToUse.metadata.partitionColumns, snapshotToUse.allFiles.toDF(), convertedFilterExpr)
      .as[AddFile](SingleAction.addFileEncoder)
      .collect().par.map { f =>
        logInfo(s"selected delta file ${f.path} under $rootPath")
        fs.getFileStatus(new Path(rootPath, f.path))
      }.toList.asJava
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