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

import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf

class AMTAllFilesInCrcSuite extends AMTCheckpointTestBase {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_ENABLED.key, "true")
    .set(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_VERIFICATION_MODE_ENABLED.key, "true")

  private def crcThreshold: Int =
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_THRESHOLD_FILES)

  testAcrossAMTCheckpointScenarios(
      "root-only AMT persists allFiles in the CRC without a back reference",
      "amt_crc_rootonly")(
      setup = name => sql(s"INSERT INTO $name VALUES (1)"),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (2)"))) { context =>
    val snapshot = context.postCheckpointSnapshot
    assert(context.provider.leaves.isEmpty,
      "a small AMT tree must be root-only (root-resident or single-manifest promoted).")

    // State reconstruction leaves every root-resident file unstamped.
    val live = snapshot.allFiles.collect()
    assert(live.nonEmpty && live.forall(_.backReference.isEmpty),
      "root-resident AMT files must carry no back reference.")

    // The CRC written for this version must carry those same files, still unstamped.
    val crc = snapshot.deltaLog.readChecksum(snapshot.version).getOrElse(
      fail(s"expected a CRC at version ${snapshot.version}."))
    val crcAdds = crc.allFiles.getOrElse(
      fail("a root-only AMT tree must persist allFiles in its CRC."))
    assert(crcAdds.nonEmpty && crcAdds.forall(_.backReference.isEmpty),
      "no AddFile persisted in a root-only AMT CRC may carry a back reference.")
    assert(crcAdds.map(_.path).toSet == live.map(_.path).toSet,
      "CRC allFiles must match state reconstruction exactly.")

    // And the CRC verifies clean (byte-identical) against a fresh state reconstruction.
    assert(snapshot.validateFileListAgainstCRC(crc, contextOpt = Some("AMTAllFilesInCrcSuite")),
      "a root-only AMT CRC must agree with state reconstruction.")
  }

  test("AMT tree with leaf manifests keeps its files out of the CRC") {
    withSQLConf(leafPackingConfs: _*) {
      withTable("amt_crc_leaves") {
        createAMTTable("amt_crc_leaves", checkpointInterval = Int.MaxValue)
        val deltaLog = deltaLogForName("amt_crc_leaves")
        commitCheckpoint(deltaLog, incremental = false) // bootstrap full checkpoint
        appendRowsAsSeparateFiles("amt_crc_leaves", numFiles = leafPackedFiles)
        commitCheckpoint(deltaLog, incremental = false)
        val snapshot = deltaLog.update()
        val provider = amtProvider(snapshot).getOrElse(fail("table must be AMT-backed."))
        assertLeafCount(provider.leaves)
        assert(snapshot.numOfFiles <= crcThreshold,
          "exclusion must be driven by leaves, not by the file-count threshold.")

        val live = snapshot.allFiles.collect()
        assert(live.nonEmpty && live.forall(_.backReference.isDefined),
          "leaf-resident AMT files must be stamped with a back reference.")

        assert(deltaLog.readChecksum(snapshot.version).flatMap(_.allFiles).isEmpty,
          "a leaf-bearing AMT tree must not persist allFiles in its CRC.")
      }
    }
  }

  test("non-AMT CRC never carries a back reference") {
    withTable("plain_crc") {
      sql("CREATE TABLE plain_crc (id INT) USING DELTA")
      sql("INSERT INTO plain_crc VALUES (1)")
      sql("INSERT INTO plain_crc VALUES (2)")

      val deltaLog = deltaLogForName("plain_crc")
      val snapshot = deltaLog.update()
      assert(amtProvider(snapshot).isEmpty, "control table must NOT be AMT-backed.")

      val crc = deltaLog.readChecksum(snapshot.version).getOrElse(
        fail(s"expected a CRC at version ${snapshot.version}."))
      val crcAdds = crc.allFiles.getOrElse(
        fail("a small non-AMT table should still persist allFiles in its CRC."))
      assert(crcAdds.nonEmpty && crcAdds.forall(_.backReference.isEmpty),
        "a non-AMT table must never persist a back reference in its CRC.")
    }
  }

  test("shrink from multi-leaf to root-only strips stale back references from the CRC") {
    withSQLConf(leafPackingConfs: _*) {
      withTable("amt_crc_shrink") {
        createAMTTable("amt_crc_shrink", checkpointInterval = Int.MaxValue)
        val deltaLog = deltaLogForName("amt_crc_shrink")
        commitCheckpoint(deltaLog, incremental = false) // bootstrap full checkpoint

        appendRowsAsSeparateFiles("amt_crc_shrink", numFiles = leafPackedFiles)
        commitCheckpoint(deltaLog, incremental = false)
        val snapshotBackedByMultiLeafAMT = deltaLog.update()
        assert(amtProvider(snapshotBackedByMultiLeafAMT).exists(_.leaves.size >= 2),
          "the tree must have multiple leaf manifests before the shrink.")
        assert(snapshotBackedByMultiLeafAMT.allFiles.collect().forall(_.backReference.isDefined),
          "leaf-resident files must be stamped with a back reference.")

        // Delete every row but id 0, leaving a single live file, then re-materialize. The full
        // checkpoint promotes to a root-only tree.
        sql("DELETE FROM amt_crc_shrink WHERE id >= 1")
        commitCheckpoint(deltaLog, incremental = false)
        val snapshotBackedByNoLeafAMT = deltaLog.update()
        assert(amtProvider(snapshotBackedByNoLeafAMT).exists(_.leaves.isEmpty),
          "the tree must be root-only after the shrink.")
        val live = snapshotBackedByNoLeafAMT.allFiles.collect()
        assert(live.nonEmpty && live.forall(_.backReference.isEmpty),
          "root-resident files must not be stamped after the shrink.")

        val crc = deltaLog.readChecksum(snapshotBackedByNoLeafAMT.version).getOrElse(
          fail(s"expected a CRC at version ${snapshotBackedByNoLeafAMT.version}."))
        val crcAdds = crc.allFiles.getOrElse(
          fail("a root-only AMT tree must persist allFiles in its CRC."))
        assert(crcAdds.nonEmpty && crcAdds.forall(_.backReference.isEmpty),
          "the shrink must strip stale leaf back references from the CRC.")
        assert(snapshotBackedByNoLeafAMT.validateFileListAgainstCRC(
            crc, contextOpt = Some("amt_crc_shrink")),
          "a root-only CRC produced by a shrink must verify clean.")
      }
    }
  }
}
