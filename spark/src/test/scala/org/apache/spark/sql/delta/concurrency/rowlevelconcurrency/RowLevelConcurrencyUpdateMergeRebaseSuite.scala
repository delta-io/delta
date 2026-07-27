/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.concurrency.rowlevelconcurrency

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{Action, AddFile, DeletionVectorDescriptor, RemoveFile}
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Unit tests for [[RowLevelConcurrency.tryRebase]] focused on the
 * UPDATE/MERGE action shape: the loser may emit additional `AddFile`s with paths that
 * differ from any `RemoveFile.path`, representing post-image rewritten rows. These
 * post-image files must pass through the rebase unmodified so the downstream
 * `reassignOverlappingRowIds` phase can assign fresh baseRowIds to them.
 */
class RowLevelConcurrencyUpdateMergeRebaseSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  // ---------- helpers ----------

  private def bitmap(rows: Long*): RoaringBitmapArray = {
    val b = new RoaringBitmapArray()
    rows.foreach(b.add)
    b
  }

  private def inlineDv(rows: Long*): DeletionVectorDescriptor = {
    val b = bitmap(rows: _*)
    if (b.isEmpty) {
      DeletionVectorDescriptor.EMPTY
    } else {
      val bytes = b.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
      DeletionVectorDescriptor.inlineInLog(bytes, b.cardinality)
    }
  }

  private def addFile(
      path: String,
      dv: DeletionVectorDescriptor = DeletionVectorDescriptor.EMPTY,
      baseRowId: Option[Long] = None): AddFile =
    AddFile(
      path = path,
      partitionValues = Map.empty,
      size = 1L,
      modificationTime = 1L,
      dataChange = true,
      stats = "{\"numRecords\": 100}",
      deletionVector = dv,
      baseRowId = baseRowId)

  private def removeFile(path: String, dv: DeletionVectorDescriptor): RemoveFile =
    RemoveFile(
      path = path,
      deletionTimestamp = Some(1L),
      dataChange = true,
      deletionVector = dv)

  private def newHadoopConf(): org.apache.hadoop.conf.Configuration = {
    // scalastyle:off deltahadoopconfiguration
    spark.sessionState.newHadoopConf()
    // scalastyle:on deltahadoopconfiguration
  }

  private def MAX_DV_BYTES = 1024L * 1024L

  // ---------- UPDATE shape: post-image AddFile preserved ----------

  test("tryRebase: UPDATE shape -- post-image AddFile (different path) is preserved") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()

      // UPDATE produces:
      //   RemoveFile("data1.parquet")  -- original file removed
      //   AddFile("data1.parquet", DV) -- same file re-added with DV marking updated rows
      //   AddFile("data1_v2.parquet")  -- post-image: rewritten rows in a new file
      val priorDv = inlineDv(0L)
      val winnerDv = inlineDv(0L, 1L)  // winner deleted row 1
      val loserDv = inlineDv(0L, 5L)   // loser deleted row 5

      val postImageAddFile = addFile("data1_v2.parquet", baseRowId = Some(123L))
      val loserActions: Seq[Action] = Seq(
        removeFile("data1.parquet", priorDv),
        addFile("data1.parquet", loserDv),
        postImageAddFile)

      val winnerAdded = Seq(addFile("data1.parquet", winnerDv))
      val winnerRemoved = Seq(removeFile("data1.parquet", priorDv))

      val result = RowLevelConcurrency.tryRebase(
        loserActions, winnerAdded, winnerRemoved, tablePath, hadoopConf, MAX_DV_BYTES)

      assert(result.resolvedFileCount == 1)
      assert(result.numDvFilesWritten == 1)

      // Post-image file must still be present in the new action set, unchanged.
      val newAddFiles = result.newActions.collect { case a: AddFile => a }
      val newAddPaths = newAddFiles.map(_.path).toSet
      assert(newAddPaths.contains("data1_v2.parquet"),
        s"Post-image AddFile missing from rebased actions: $newAddPaths")
      assert(newAddFiles.exists(a => a.path == "data1_v2.parquet" && a.baseRowId == Some(123L)),
        "Post-image AddFile must be preserved verbatim including baseRowId")
    }
  }

  test("tryRebase: MERGE shape -- multiple post-image AddFiles all preserved") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()

      val priorDv = inlineDv()
      val winnerDv = inlineDv(1L)
      val loserDv = inlineDv(5L)

      // MERGE emitting a same-path DV mod plus two new-path post-image files. Only the
      // same-path pair is rebased here; new-path files must survive untouched.
      val postImage1 = addFile("merge_out_1.parquet", baseRowId = Some(200L))
      val postImage2 = addFile("merge_out_2.parquet", baseRowId = Some(300L))
      val loserActions: Seq[Action] = Seq(
        removeFile("target.parquet", priorDv),
        addFile("target.parquet", loserDv),
        postImage1,
        postImage2)

      val winnerAdded = Seq(addFile("target.parquet", winnerDv))
      val winnerRemoved = Seq(removeFile("target.parquet", priorDv))

      val result = RowLevelConcurrency.tryRebase(
        loserActions, winnerAdded, winnerRemoved, tablePath, hadoopConf, MAX_DV_BYTES)

      assert(result.resolvedFileCount == 1)
      val newAddPaths = result.newActions.collect { case a: AddFile => a.path }.toSet
      assert(newAddPaths.contains("merge_out_1.parquet"))
      assert(newAddPaths.contains("merge_out_2.parquet"))
      assert(newAddPaths.contains("target.parquet"))  // The rebased same-path AddFile
    }
  }

  test("tryRebase: rebased same-path AddFile inherits winner baseRowId") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()

      val priorDv = inlineDv(0L)
      val winnerDv = inlineDv(0L, 1L)
      val loserDv = inlineDv(0L, 5L)

      // Winner assigned baseRowId=42 to this file in its commit
      val winnerAddFile = addFile("data.parquet", winnerDv, baseRowId = Some(42L))
      // Loser proposes a different baseRowId=99, which the downstream row-id
      // reassignment phase is allowed to rewrite.
      val loserAddFile = addFile("data.parquet", loserDv, baseRowId = Some(99L))

      val loserActions: Seq[Action] = Seq(
        removeFile("data.parquet", priorDv),
        loserAddFile)
      val winnerAdded = Seq(winnerAddFile)
      val winnerRemoved = Seq(removeFile("data.parquet", priorDv))

      val result = RowLevelConcurrency.tryRebase(
        loserActions, winnerAdded, winnerRemoved, tablePath, hadoopConf, MAX_DV_BYTES)

      assert(result.resolvedFileCount == 1)
      val rebasedAdd = result.newActions
        .collect { case a: AddFile => a }
        .find(_.path == "data.parquet")
        .get
      // The rebased same-path AddFile must carry the
      // winner's baseRowId (the file's physical contents are unchanged from the winner's
      // post-image; only the DV layered on top of it has been replaced).
      assert(rebasedAdd.baseRowId == Some(42L),
        s"Expected baseRowId from winner (42L) but got ${rebasedAdd.baseRowId}")
    }
  }

  test("tryRebase: non-shared post-image AddFile + no shared files = no-op") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()

      // UPDATE writes only a post-image file; no shared paths with winner.
      val loserActions: Seq[Action] = Seq(addFile("only_post_image.parquet"))
      val winnerAdded = Seq(addFile("winner_only.parquet"))
      val winnerRemoved = Seq.empty[RemoveFile]

      val result = RowLevelConcurrency.tryRebase(
        loserActions, winnerAdded, winnerRemoved, tablePath, hadoopConf, MAX_DV_BYTES)

      assert(result.resolvedFileCount == 0)
      assert(result.newActions == loserActions, "No shared paths means actions unchanged")
    }
  }
}
