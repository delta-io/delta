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

package org.apache.spark.sql.delta.storage.dv

import java.io.File

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{AddFile, DeletionVectorDescriptor, RemoveFile}
import org.apache.spark.sql.delta.deletionvectors.DeletionVectorsSuite
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class DeletionVectorFileSizeSuite extends QueryTest
    with SharedSparkSession
    with DeltaSQLTestUtils {
  private def getAddAndRemoveFilesFromCommitVersion(
      deltaLog: DeltaLog,
      commitVersion: Long): (Seq[AddFile], Seq[RemoveFile]) = {
    require(commitVersion <= deltaLog.update().version,
      "Commit version should be less than or equal to the current version")
    val changes = deltaLog.getChanges(commitVersion, commitVersion, failOnDataLoss = true)
    val (changesItrForAddFiles, changesItrForRemoveFiles) = changes.duplicate
    val addFiles: Seq[AddFile] =
       changesItrForAddFiles.flatMap(_._2.collect { case a: AddFile => a }).toSeq
    val removeFiles: Seq[RemoveFile] =
       changesItrForRemoveFiles.flatMap(_._2.collect { case r: RemoveFile => r }).toSeq
    (addFiles, removeFiles)
  }

  private def getDeletionVectorFilePath(
    dvDescriptor: DeletionVectorDescriptor,
    tableDataPath: String
  ): Path = {
    val path = dvDescriptor.absolutePath(new Path(tableDataPath))
    path
  }

  private def getFileSizeInBytes(
      absoluteFilePath: Path,
      hadoopConf: org.apache.hadoop.conf.Configuration): Long = {
    val file = new File(absoluteFilePath.toString)
    assert(file.exists())

    val fs = absoluteFilePath.getFileSystem(hadoopConf)
    val fileStatus = fs.getFileStatus(absoluteFilePath)
    fileStatus.getLen
  }

  test("Bin Packing should take the size of existing DVs into account") {
    withTempDir { tempDir =>
      val source = new File(DeletionVectorsSuite.table1Path)
      val target = new File(tempDir, "deleteTest")

      // Copy the source table with existing table layout to a temporary directory
      FileUtils.copyDirectory(source, target)

      val (deltaLog, snapshot) =
        DeltaLog.forTableWithSnapshot(spark, new Path(target.getAbsolutePath))
      assert(snapshot.version === 4, "Table should exist")
      // All existing individual DVs either have 34 or 36 bytes, corresponding 1 or 2 deleted rows.
      val priorAddFiles = snapshot.allFiles.collect()
      priorAddFiles.forall(a => a.deletionVector == null
        || (a.deletionVector.sizeInBytes == 34 || a.deletionVector.sizeInBytes == 36))
      assert(priorAddFiles.count(_.deletionVector != null) === 8,
        "8 AddFiles with DVs expected")

      val targetPackingFileSizeInBytes = 110
      withSQLConf(
          DeltaSQLConf.DELETION_VECTOR_PACKING_TARGET_SIZE.key ->
          targetPackingFileSizeInBytes.toString) {
        // Delete some rows to trigger the creation of new DVs.
        sql(s"DELETE FROM delta.`${target.getAbsolutePath}` WHERE value IN (255, 303, 707, 1905)")
        val (addFiles, removeFiles) =
          getAddAndRemoveFilesFromCommitVersion(deltaLog, deltaLog.update().version)
        assert(addFiles.size === 3, "Deletion vector added to 3 different AddFiles")
        assert(addFiles.forall(_.deletionVector != null),
          "Deletion should have used DVs and not rewrite any files")
        val removeFilesPaths = removeFiles.map(_.path).toSet
        assert(priorAddFiles.filter(a => removeFilesPaths.contains(a.path))
          .count(_.deletionVector != null) === 2, "2 of the AddFiles had existing DVs")

        // Get the file sizes of the DV files added by this commit.
        val dvFileSizes = addFiles.map(_.deletionVector)
          .map(dv => getDeletionVectorFilePath(dv, target.getAbsolutePath))
          .toSet
          .map(path => getFileSizeInBytes(path, deltaLog.newDeltaHadoopConf()))
        assert(addFiles.forall(a => a.deletionVector.sizeInBytes <= targetPackingFileSizeInBytes),
          "the individual DVs can each fit in their own file if needed")
        assert(dvFileSizes.forall(_ <= targetPackingFileSizeInBytes),
          "The target file size should be respected when individual DVs fit under the file size")
      }
    }
  }
}
