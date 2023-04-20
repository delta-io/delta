/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta

import java.io.File
import java.util.UUID

import org.apache.spark.sql.delta.DeltaOperations.Truncate
import org.apache.spark.sql.delta.actions.{Action, AddFile, DeletionVectorDescriptor, RemoveFile}
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.delta.util.PathWithFileSystem
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, QueryTest, SparkSession}
import org.apache.spark.sql.test.SharedSparkSession

/** Collection of test utilities related with persistent Deletion Vectors. */
trait DeletionVectorsTestUtils extends QueryTest with SharedSparkSession {

  def enableDeletionVectorsForDeletes(spark: SparkSession, enabled: Boolean = true): Unit = {
    val enabledStr = enabled.toString
    spark.conf
      .set(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey, enabledStr)
    spark.conf.set(DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key, enabledStr)
  }

  def testWithDVs(testName: String, testTags: org.scalatest.Tag*)(thunk: => Unit): Unit = {
    test(testName, testTags : _*) {
      withDeletionVectorsEnabled() {
        thunk
      }
    }
  }

  /** Run a thunk with Deletion Vectors enabled/disabled. */
  def withDeletionVectorsEnabled(enabled: Boolean = true)(thunk: => Unit): Unit = {
    val enabledStr = enabled.toString
    withSQLConf(
      DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> enabledStr,
      DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> enabledStr) {
      thunk
    }
  }

  /** Helper to run 'fn' with a temporary Delta table. */
  def withTempDeltaTable(
      dataDF: DataFrame,
      partitionBy: Seq[String] = Seq.empty,
      enableDVs: Boolean = true,
      conf: Seq[(String, String)] = Nil)
      (fn: (() => io.delta.tables.DeltaTable, DeltaLog) => Unit): Unit = {
    withTempPath { path =>
      val tablePath = new Path(path.getAbsolutePath)
      withSQLConf(conf: _*) {
        dataDF.write
          .option(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, enableDVs.toString)
          .partitionBy(partitionBy: _*)
          .format("delta")
          .save(tablePath.toString)
      }
      // DeltaTable hangs on to the DataFrame it is created with for the entire object lifetime.
      // That means subsequent `targetTable.toDF` calls will return the same snapshot.
      // The DV tests are generally written assuming `targetTable.toDF` would return a new snapshot.
      // So create a function here instead of a n instance, so `targetTable().toDF`
      // will actually provide a new snapshot.
      val targetTable =
        () => io.delta.tables.DeltaTable.forPath(tablePath.toString)
      val targetLog = DeltaLog.forTable(spark, tablePath)
      fn(targetTable, targetLog)
    }
  }

  /** Helper that verifies whether a defined number of DVs exist */
  def verifyDVsExist(targetLog: DeltaLog, filesWithDVsSize: Int): Unit = {
    val filesWithDVs = getFilesWithDeletionVectors(targetLog)
    assert(filesWithDVs.size === filesWithDVsSize)
    assertDeletionVectorsExist(targetLog, filesWithDVs)
  }

  /** Returns all [[AddFile]] actions of a Delta table that contain Deletion Vectors. */
  def getFilesWithDeletionVectors(log: DeltaLog): Seq[AddFile] =
    log.update().allFiles.collect().filter(_.deletionVector != null).toSeq

  /** Lists the Deletion Vectors files of a table. */
  def listDeletionVectors(log: DeltaLog): Seq[File] = {
    val dir = new File(log.dataPath.toUri.getPath)
    dir.listFiles().filter(_.getName.startsWith(
      DeletionVectorDescriptor.DELETION_VECTOR_FILE_NAME_CORE))
  }

  /** Helper to check that the Deletion Vectors of the provided file actions exist on disk. */
  def assertDeletionVectorsExist(log: DeltaLog, filesWithDVs: Seq[AddFile]): Unit = {
    val tablePath = new Path(log.dataPath.toUri.getPath)
    for (file <- filesWithDVs) {
      val dv = file.deletionVector
      assert(dv != null)
      assert(dv.isOnDisk && !dv.isInline)
      assert(dv.offset.isDefined)

      // Check that DV exists.
      val dvPath = dv.absolutePath(tablePath)
      val dvPathStr = DeletionVectorStore.pathToString(dvPath)
      assert(new File(dvPathStr).exists(), s"DV not found $dvPath")

      // Check that cardinality is correct.
      val bitmap = newDVStore.read(dvPath, dv.offset.get, dv.sizeInBytes)
      assert(dv.cardinality === bitmap.cardinality)
    }
  }

  /** Enable persistent Deletion Vectors in a Delta table. */
  def enableDeletionVectorsInTable(tablePath: Path, enable: Boolean): Unit =
    spark.sql(
      s"""ALTER TABLE delta.`$tablePath`
         |SET TBLPROPERTIES ('${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key}' = '$enable')
         |""".stripMargin)

  /** Enable persistent Deletion Vectors in a Delta table. */
  def enableDeletionVectorsInTable(deltaLog: DeltaLog, enable: Boolean = true): Unit =
    enableDeletionVectorsInTable(deltaLog.dataPath, enable)

  // ======== HELPER METHODS TO WRITE DVs ==========
  /** Helper method to remove the specified rows in the given file using DVs */
  protected def removeRowsFromFileUsingDV(
      log: DeltaLog,
      addFile: AddFile,
      rowIds: Seq[Long]): Seq[Action] = {
    val dv = RoaringBitmapArray(rowIds: _*)
    writeFileWithDV(log, addFile, dv)
  }

  /** Utility method to remove a ratio of rows from the given file */
  protected def deleteRows(
      log: DeltaLog, file: AddFile, approxPhyRows: Long, ratioOfRowsToDelete: Double): Unit = {
    val numRowsToDelete =
      Math.ceil(ratioOfRowsToDelete * file.numPhysicalRecords.getOrElse(approxPhyRows)).toInt
    removeRowsFromFile(log, file, Seq.range(0, numRowsToDelete))
  }

  /** Utility method to remove the given rows from the given file using DVs */
  protected def removeRowsFromFile(
      log: DeltaLog, addFile: AddFile, rowIndexesToRemove: Seq[Long]): Unit = {
    val txn = log.startTransaction()
    val actions = removeRowsFromFileUsingDV(log, addFile, rowIndexesToRemove)
    txn.commit(actions, Truncate())
  }

  protected def serializeRoaringBitmapArrayWithDefaultFormat(
      dv: RoaringBitmapArray): Array[Byte] = {
    val serializationFormat = RoaringBitmapArrayFormat.Portable
    dv.serializeAsByteArray(serializationFormat)
  }

  /**
   * Produce a new [[AddFile]] that will store `dv` in the log using default settings for choosing
   * inline or on-disk storage.
   *
   * Also returns the corresponding [[RemoveFile]] action for `currentFile`.
   *
   * TODO: Always on-disk for now. Inline support comes later.
   */
  protected def writeFileWithDV(
      log: DeltaLog,
      currentFile: AddFile,
      dv: RoaringBitmapArray): Seq[Action] = {
    writeFileWithDVOnDisk(log, currentFile, dv)
  }

  /**
   * Produce a new [[AddFile]] that will reference the `dv` in the log while storing it on-disk.
   *
   * Also returns the corresponding [[RemoveFile]] action for `currentFile`.
   */
  protected def writeFileWithDVOnDisk(
      log: DeltaLog,
      currentFile: AddFile,
      dv: RoaringBitmapArray): Seq[Action] = writeFilesWithDVsOnDisk(log, Seq((currentFile, dv)))

  protected def withDVWriter[T](
      log: DeltaLog,
      dvFileID: UUID)(fn: DeletionVectorStore.Writer => T): T = {
    val dvStore = newDVStore
    // scalastyle:off deltahadoopconfiguration
    val conf = spark.sessionState.newHadoopConf()
    // scalastyle:on deltahadoopconfiguration
    val tableWithFS = PathWithFileSystem.withConf(log.dataPath, conf)
    val dvPath =
      DeletionVectorStore.assembleDeletionVectorPathWithFileSystem(tableWithFS, dvFileID)
    val writer = dvStore.createWriter(dvPath)
    try {
      fn(writer)
    } finally {
      writer.close()
    }
  }

  /**
   * Produce new [[AddFile]] actions that will reference associated DVs in the log while storing
   * all DVs in the same file on-disk.
   *
   * Also returns the corresponding [[RemoveFile]] actions for the original file entries.
   */
  protected def writeFilesWithDVsOnDisk(
      log: DeltaLog,
      filesWithDVs: Seq[(AddFile, RoaringBitmapArray)]): Seq[Action] = {
    val dvFileId = UUID.randomUUID()
    withDVWriter(log, dvFileId) { writer =>
      filesWithDVs.flatMap { case (currentFile, dv) =>
        val range = writer.write(serializeRoaringBitmapArrayWithDefaultFormat(dv))
        val dvData = DeletionVectorDescriptor.onDiskWithRelativePath(
          id = dvFileId,
          sizeInBytes = range.length,
          cardinality = dv.cardinality,
          offset = Some(range.offset))
        val (add, remove) = currentFile.removeRows(
          dvData
        )
        Seq(add, remove)
      }
    }
  }

  /**
   * Removes the `numRowsToRemovePerFile` from each file via DV.
   * Returns the total number of rows removed.
   */
  protected def removeRowsFromAllFilesInLog(
      log: DeltaLog,
      numRowsToRemovePerFile: Long): Long = {
    var numFiles: Option[Int] = None
    // This is needed to make the manual commit work correctly, since we are not actually
    // running a command that produces metrics.
    withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false") {
      val txn = log.startTransaction()
      val allAddFiles = txn.snapshot.allFiles.collect()
      numFiles = Some(allAddFiles.length)
      val bitmap = RoaringBitmapArray(0L until numRowsToRemovePerFile: _*)
      val actions = allAddFiles.flatMap { file =>
        if (file.numPhysicalRecords.isDefined) {
          // Only when stats are enabled. Can't check when stats are disabled
          assert(file.numPhysicalRecords.get > numRowsToRemovePerFile)
        }
        writeFileWithDV(log, file, bitmap)
      }
      txn.commit(actions, DeltaOperations.Delete(predicate = Seq.empty))
    }
    numFiles.get * numRowsToRemovePerFile
  }

  def newDVStore(): DeletionVectorStore = {
    // scalastyle:off deltahadoopconfiguration
    DeletionVectorStore.createInstance(spark.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration
  }

  /**
   * Updates an [[AddFile]] with a [[DeletionVectorDescriptor]].
   */
  protected def updateFileDV(
      addFile: AddFile,
      dvDescriptor: DeletionVectorDescriptor): (AddFile, RemoveFile) = {
    addFile.removeRows(
      dvDescriptor
    )
  }

  /**
   * Creates a [[DeletionVectorDescriptor]] from an [[RoaringBitmapArray]]
   */
  protected def writeDV(
      log: DeltaLog,
      bitmapArray: RoaringBitmapArray): DeletionVectorDescriptor = {
    val dvFileId = UUID.randomUUID()
    withDVWriter(log, dvFileId) { writer =>
      val range = writer.write(serializeRoaringBitmapArrayWithDefaultFormat(bitmapArray))
      DeletionVectorDescriptor.onDiskWithRelativePath(
        id = dvFileId,
        sizeInBytes = range.length,
        cardinality = bitmapArray.cardinality,
        offset = Some(range.offset))
    }
  }
}
