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

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{Action, AddFile, FileAction, RemoveFile}
import org.apache.hadoop.fs.Path

/**
 * Tests for reconstructing an inline manifest commit's file actions from the AMT it wrote.
 *
 * Every inline manifest commit is also checked against its own tree right after it is written
 * (`OptimisticTransaction.verifyAmtCommitReconstruction`), which fails tests on a mismatch, so each
 * commit below exercises that check as well as the reconstruction read back from the committed
 * tree.
 */
class AMTCommitReconstructionSuite extends AMTIncrementalWriteTestBase {

  /** Runs `body` on an AMT table whose files 1 to [[leafPackedFiles]] are packed into leaves. */
  private def withLeafPackedAmtTable(body: DeltaLog => Unit): Unit =
    withSQLConf(leafPackingConfs: _*) {
      withAmtTable { deltaLog =>
        setupAmt(
          deltaLog, entriesPerLeaf = entriesPerLeaf, initialIdRangeInLeaf = 1 to leafPackedFiles)
        body(deltaLog)
      }
    }

  private def fileName(action: FileAction): String = new Path(action.path).getName

  /**
   * Commits `actions` as an inline manifest commit, then reconstructs its file actions from the
   * tree it committed. Returns the commit's version and the reconstructed actions.
   */
  private def commitAndReconstruct(
      deltaLog: DeltaLog,
      actions: Seq[Action],
      dataChange: Boolean = true,
      commitTimestamp: Long = 0L): (Long, Seq[FileAction]) = {
    val version = deltaLog.update().version + 1
    withInline { commitAmt(deltaLog, actions) }
    val checkpoint = checkpointAt(deltaLog, version)
      .getOrElse(fail(s"v$version must be a manifest commit"))
    assert(checkpoint.contentRoot.version == version,
      s"the tree of v$version must describe v$version, got ${checkpoint.contentRoot.version}")
    val provider = AMTCheckpointProvider.fromCheckpoint(deltaLog, checkpoint, version)
    (version, provider.reconstructFileActions(deltaLog, dataChange, commitTimestamp))
  }

  /**
   * Commits `actions` as an inline manifest commit and asserts its tree reconstructs the expected
   * number of AddFiles and RemoveFiles. The commit-time check already compares the reconstructed
   * file identities with the committed ones, but it skips some commits and compares sets, so the
   * counts keep each test asserting on its own and also catch duplicate actions.
   */
  private def assertCommitReconstructs(
      deltaLog: DeltaLog,
      actions: Seq[Action],
      expectedAdds: Int,
      expectedRemoves: Int): Unit = {
    val (version, reconstructed) = commitAndReconstruct(deltaLog, actions)
    val adds = reconstructed.count(_.isInstanceOf[AddFile])
    val removes = reconstructed.count(_.isInstanceOf[RemoveFile])
    assert(adds == expectedAdds && removes == expectedRemoves,
      s"v$version: expected $expectedAdds add(s) / $expectedRemoves remove(s), got " +
        s"$adds / $removes from $reconstructed")
  }

  test("reconstructs AddFiles that stay in the root") {
    withLeafPackedAmtTable { deltaLog =>
      assertCommitReconstructs(
        deltaLog, (100 to 102).map(fakeAdd), expectedAdds = 3, expectedRemoves = 0)
    }
  }

  test("reconstructs AddFiles spilled into new leaves") {
    withLeafPackedAmtTable { deltaLog =>
      val leavesBefore = leafPointers(deltaLog.update()).size
      assertCommitReconstructs(
        deltaLog, (100 to 124).map(fakeAdd), expectedAdds = 25, expectedRemoves = 0)
      assert(leafPointers(deltaLog.update()).size > leavesBefore,
        "the insert must spill at least one new leaf for this scenario to be meaningful.")
    }
  }

  test("reconstructs a RemoveFile from a leaf's deleted positions") {
    withLeafPackedAmtTable { deltaLog =>
      assertCommitReconstructs(
        deltaLog, Seq(removeOf(deltaLog, 1)), expectedAdds = 0, expectedRemoves = 1)
    }
  }

  test("reconstructs a RemoveFile from a root tombstone") {
    withLeafPackedAmtTable { deltaLog =>
      // A root-resident file, deleted by the next manifest commit without a back reference, so
      // the delete becomes a DELETED root DATA entry.
      withInline { commitAmt(deltaLog, Seq(fakeAdd(31))) }
      assertCommitReconstructs(
        deltaLog, Seq(removeOf(deltaLog, 31)), expectedAdds = 0, expectedRemoves = 1)
    }
  }

  test("reconstructs a DV update of a leaf file from its replaced positions") {
    withLeafPackedAmtTable { deltaLog =>
      assertCommitReconstructs(
        deltaLog, removeAndReAddWithDV(deltaLog, Left(1)), expectedAdds = 1, expectedRemoves = 1)
    }
  }

  test("reconstructs a DV update of a root file") {
    withLeafPackedAmtTable { deltaLog =>
      withInline { commitAmt(deltaLog, Seq(fakeAdd(31))) }
      assertCommitReconstructs(
        deltaLog, removeAndReAddWithDV(deltaLog, Left(31)), expectedAdds = 1, expectedRemoves = 1)
    }
  }

  test("reconstructs an insert, a leaf delete, and a leaf DV update in one commit") {
    withLeafPackedAmtTable { deltaLog =>
      val actions =
        Seq[Action](removeOf(deltaLog, 1)) ++
          removeAndReAddWithDV(deltaLog, Left(2)) ++
          Seq(fakeAdd(100))
      assertCommitReconstructs(deltaLog, actions, expectedAdds = 2, expectedRemoves = 2)
    }
  }

  test("reconstructs deletes across leaves") {
    withLeafPackedAmtTable { deltaLog =>
      val leaves = leafToAddFileMap(deltaLog).values.toSeq
      assert(leaves.size >= 2, "scenario needs at least two leaves.")
      val actions = Seq[Action](leaves(0).head.remove, leaves(1).head.remove)
      assertCommitReconstructs(deltaLog, actions, expectedAdds = 0, expectedRemoves = 2)
    }
  }

  test("reconstructs DV updates across leaves") {
    withLeafPackedAmtTable { deltaLog =>
      val leaves = leafToAddFileMap(deltaLog).values.toSeq
      assert(leaves.size >= 2, "scenario needs at least two leaves.")
      val actions =
        removeAndReAddWithDV(deltaLog, Right(leaves(0).head)) ++
          removeAndReAddWithDV(deltaLog, Right(leaves(1).head))
      assertCommitReconstructs(deltaLog, actions, expectedAdds = 2, expectedRemoves = 2)
    }
  }

  test("reconstructs a fully deleted leaf") {
    withLeafPackedAmtTable { deltaLog =>
      val leaf = leafToAddFileMap(deltaLog).values.head
      assertCommitReconstructs(
        deltaLog, leaf.map(_.remove), expectedAdds = 0, expectedRemoves = leaf.size)
    }
  }

  test("reconstructs changes across leaves and the root in one commit") {
    withLeafPackedAmtTable { deltaLog =>
      withInline { commitAmt(deltaLog, Seq(fakeAdd(31))) }
      val leaves = leafToAddFileMap(deltaLog).values.toSeq
      assert(leaves.size >= 2, "scenario needs at least two leaves.")
      // Leaf A: a delete. Leaf B: a DV update. Plus a root delete and an insert.
      val actions =
        Seq[Action](leaves(0).head.remove) ++
          removeAndReAddWithDV(deltaLog, Right(leaves(1).head)) ++
          Seq[Action](removeOf(deltaLog, 31), fakeAdd(100))
      assertCommitReconstructs(deltaLog, actions, expectedAdds = 2, expectedRemoves = 3)
    }
  }

  test("stamps the given dataChange and commit timestamp on every reconstructed action") {
    withLeafPackedAmtTable { deltaLog =>
      // A compaction: the tree records its delete and insert even though neither changes data.
      val actions = Seq[Action](
        removeOf(deltaLog, 1).copy(dataChange = false),
        fakeAdd(200, dataChange = false))
      val (_, reconstructed) =
        commitAndReconstruct(deltaLog, actions, dataChange = false, commitTimestamp = 1234L)
      assert(reconstructed.map(a => (fileName(a), a.isInstanceOf[AddFile])).toSet ===
        Set((fakeAdd(1).path, false), (fakeAdd(200).path, true)))
      assert(reconstructed.forall(!_.dataChange), s"got $reconstructed")
      assert(reconstructed.collect { case r: RemoveFile => r.deletionTimestamp } ===
        Seq(Some(1234L)))
    }
  }
}
