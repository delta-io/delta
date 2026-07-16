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

package org.apache.spark.sql.delta.amt

import org.apache.spark.sql.delta.{DeletionVectorsTestUtils, DeltaLog, DeltaOperations}
import org.apache.spark.sql.delta.actions.{AddFile, DeletionVectorDescriptor}
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.Row

/**
 * Verifies that [[Snapshot]] APIs correct results on AMT tables.
 */
class AMTSnapshotSuite extends AMTCheckpointTestBase with DeletionVectorsTestUtils {

  import testImplicits._

  ///////////////////////////
  // Post commit snapshot
  ///////////////////////////

  test("snapshot.allFiles reflects a DELETE that lands on the checkpoint boundary") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createAMTTable(path, checkpointInterval = 2)
      sql(s"INSERT INTO delta.`$path` VALUES (1)")   // v1: 1 file.
      sql(s"INSERT INTO delta.`$path` VALUES (2)")   // v2: emit; 2 live files.
      // Sanity: snapshot state and leaves agree at the first checkpoint.
      assert(DeltaLog.forTable(spark, path).unsafeVolatileSnapshot.allFiles.count() == 2)

      sql(s"INSERT INTO delta.`$path` VALUES (3)")   // v3: 3 live files (not a boundary).
      sql(s"DELETE FROM delta.`$path` WHERE id = 1") // v4: emit; live set drops id=1.

      val snapshot = DeltaLog.forTable(spark, path).unsafeVolatileSnapshot
      assert(snapshot.version == 4)
      assert(amtProvider(snapshot).isDefined)
      // allFiles must reflect the DELETE: the removed file is gone from the post-commit live set.
      checkAnswer(spark.read.format("delta").load(path), Seq(Row(2), Row(3)))
      // The manifest tree written at the checkpoint captures exactly the post-DELETE live files
      // (computePostCommitState applied the RemoveFile), i.e. it matches snapshot.allFiles.
      assert(currentLeafDataEntries(snapshot) == snapshot.allFiles.count(),
        "Leaf DATA entries must equal the post-commit live file count.")
    }
  }

  test("snapshot.allFiles matches leaves across insert/overwrite/delete before a checkpoint") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createAMTTable(path, checkpointInterval = 4)
      sql(s"INSERT INTO delta.`$path` VALUES (1)")            // v1.
      sql(s"INSERT INTO delta.`$path` VALUES (2)")            // v2.
      sql(s"INSERT OVERWRITE delta.`$path` VALUES (10), (20)") // v3: replaces all prior files.
      sql(s"DELETE FROM delta.`$path` WHERE id = 10")         // v4: emit; removes one.

      val snapshot = DeltaLog.forTable(spark, path).unsafeVolatileSnapshot
      assert(snapshot.version == 4)
      assert(amtProvider(snapshot).isDefined)
      checkAnswer(spark.read.format("delta").load(path), Seq(Row(20)))
      assert(snapshot.allFiles.count() == 1, "Only the surviving overwrite file should be live.")
      assert(currentLeafDataEntries(snapshot) == 1,
        "Leaves must capture exactly the surviving live file after overwrite + delete.")
    }
  }

  test("filtered scan is correct after emission (reconstruction from trimmed deltas + leaves)") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createAMTTable(path, checkpointInterval = 2)
      sql(s"INSERT INTO delta.`$path` VALUES (1)")
      sql(s"INSERT INTO delta.`$path` VALUES (2)")   // v2: emit; provider is AMT.
      sql(s"INSERT INTO delta.`$path` VALUES (3)")   // v3.
      sql(s"DELETE FROM delta.`$path` WHERE id = 1") // v4: emit again.

      // SQL data-skipping reconstruction is disabled for AMT tables, so reads go through the
      // allFiles-based reconstruction: the leaves supply state up to the checkpoint and the
      // trimmed deltas supply the rest. Each row must appear exactly once -- a double-count would
      // surface as duplicate rows or wrong counts.
      checkAnswer(spark.read.format("delta").load(path).filter("id >= 2"), Seq(Row(2), Row(3)))
      checkAnswer(
        spark.read.format("delta").load(path).groupBy().count(), Seq(Row(2L)))
      checkAnswer(spark.read.format("delta").load(path), Seq(Row(2), Row(3)))
    }
  }

  test("deletion vector round-trips through the leaves with a matching uniqueId") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createAMTTable(path, checkpointInterval = 2)
      Seq(1, 2).toDF("id").coalesce(1)
        .write.format("delta").mode("append").save(path) // v1: one file, two rows.

      // Attach a persistent DV directly rather than relying on DELETE's DV-vs-rewrite heuristic:
      // write a DV marking row 0 deleted and commit the resulting AddFile (with DV) + RemoveFile.
      // v2 is a checkpoint boundary -> emit.
      val log = DeltaLog.forTable(spark, path)
      val fileToDv = log.unsafeVolatileSnapshot.allFiles.collect()
      assert(fileToDv.length == 1, "The two rows must land in a single file.")
      val dvActions = writeFileWithDVOnDisk(log, fileToDv.head, RoaringBitmapArray(0L))
      // Disable history metrics: this is a manual commit, not a real DELETE command, so the
      // operation-metrics (e.g. numDeletedRows) the Delete op expects are not populated.
      withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false") {
        log.startTransaction().commit(dvActions, DeltaOperations.Delete(predicate = Seq.empty))
      }

      val snapshot = DeltaLog.forTable(spark, path).unsafeVolatileSnapshot
      assert(snapshot.version == 2)
      val provider = amtProvider(snapshot).getOrElse(fail("expected AMTCheckpointProvider"))
      checkAnswer(spark.read.format("delta").load(path), Seq(Row(2)))

      // The one surviving live file must carry a deletion vector in committed state.
      val committed = snapshot.allFiles.collect()
      assert(committed.length == 1)
      val committedFile = committed.head
      val committedDv = committedFile.deletionVector
      assert(committedDv != null, "The committed file must carry a deletion vector.")
      // Two physical rows, one deleted by the DV -> one logical row.
      assert(committedFile.numPhysicalRecords.contains(2L))
      assert(committedFile.numLogicalRecords.contains(1L))

      // The leaf-reconstructed AddFile must recover a DV with the SAME uniqueId, so the
      // (path, deletionVectorUniqueId) dedup key matches the committed file exactly (no
      // double-count in the reader path). uniqueId is
      // storageType + pathOrInlineDv (+ "@offset"), so compare it from the reconstructed fields.
      val dvRow = provider
        .loadActionsForStateReconstruction(spark, snapshot.deltaLog)
        .getOrElse(fail("AMT provider must contribute leaf-derived file actions."))
        .where("add is not null and add.deletionVector is not null")
        .selectExpr("add.deletionVector.storageType", "add.deletionVector.pathOrInlineDv",
          "add.deletionVector.offset")
        .collect()
      assert(dvRow.length == 1, "Exactly one leaf DATA entry must carry a DV.")
      val reconstructedDv = DeletionVectorDescriptor(
        storageType = dvRow.head.getString(0),
        pathOrInlineDv = dvRow.head.getString(1),
        offset = Option(dvRow.head.get(2)).map(_ => dvRow.head.getInt(2)),
        sizeInBytes = committedDv.sizeInBytes,
        cardinality = committedDv.cardinality)
      assert(reconstructedDv.uniqueId == committedDv.uniqueId,
        s"Reconstructed DV uniqueId ${reconstructedDv.uniqueId} must equal committed " +
          s"${committedDv.uniqueId}.")

      // The leaf-reconstructed AddFile must recover the same physical/logical record counts as the
      // committed file: `numRecords` in stats is the physical count, so it must round-trip without
      // being off by the DV cardinality.
      import org.apache.spark.sql.delta.implicits._
      val reconstructed = provider
        .loadActionsForStateReconstruction(spark, snapshot.deltaLog)
        .getOrElse(fail("AMT provider must contribute leaf-derived file actions."))
        .where("add is not null")
        .select("add.*")
        .as[AddFile]
        .collect()
      assert(reconstructed.length == 1)
      assert(reconstructed.head.numPhysicalRecords.contains(2L),
        "Reconstructed physical record count must match the committed file.")
      assert(reconstructed.head.numLogicalRecords.contains(1L),
        "Reconstructed logical record count must match the committed file.")
    }
  }
}
