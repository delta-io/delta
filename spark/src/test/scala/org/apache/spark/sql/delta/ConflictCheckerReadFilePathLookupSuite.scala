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

import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions.{Action, AddFile, Metadata}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Tests for the read file lookup used to check files removed by winning commits against the
 * files read by the current transaction, which is shared by all copies of a
 * [[CurrentTransactionInfo]] instead of being rebuilt for every winning commit.
 */
class ConflictCheckerReadFilePathLookupSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  import testImplicits._

  private val addA_P1 = AddFile("part=1/a", Map("part" -> "1"), 1, 1, dataChange = true)
  private val addB_P1 = AddFile("part=1/b", Map("part" -> "1"), 1, 1, dataChange = true)
  private val addC_P2 = AddFile("part=2/c", Map("part" -> "2"), 1, 1, dataChange = true)
  private val addD_P2 = AddFile("part=2/d", Map("part" -> "2"), 1, 1, dataChange = true)
  private val addE_P3 = AddFile("part=3/e", Map("part" -> "3"), 1, 1, dataChange = true)

  private def withLog(actions: Seq[Action])(test: DeltaLog => Unit): Unit = {
    val schema = StructType(Seq(StructField("part", StringType)))
    val metadata = Metadata(partitionColumns = Seq("part"), schemaString = schema.json)
    withTempDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir)
      log.startTransaction().commit(Seq(metadata), ManualUpdate)
      log.startTransaction().commitManually(actions :+ metadata: _*)
      test(log)
    }
  }

  private def txnInfo(snapshot: Snapshot, readFiles: Set[AddFile]): CurrentTransactionInfo =
    CurrentTransactionInfo(
      txnId = "txn",
      readPredicates = Vector.empty,
      readFiles = readFiles,
      readWholeTable = false,
      readAppIds = Set.empty,
      metadata = snapshot.metadata,
      protocol = snapshot.protocol,
      actions = Seq.empty,
      readSnapshot = snapshot,
      commitInfo = None,
      readRowIdHighWatermark = 0L,
      catalogTable = None,
      domainMetadata = Seq.empty,
      op = ManualUpdate)

  test("read file lookup is shared by copies and rebuilt when the read files change") {
    withLog(Seq(addA_P1, addB_P1, addC_P2)) { log =>
      val info = txnInfo(log.update(), readFiles = Set(addA_P1, addC_P2))
      val expected = Map(addA_P1.path -> Map("part" -> "1"), addC_P2.path -> Map("part" -> "2"))
      val lookup = info.readFilePathToPartitionValues
      assert(lookup === expected)
      assert(info.readFilePathToPartitionValues eq lookup)

      // Copies made for each winning commit reuse the lookup instead of rebuilding it.
      val copied = info.copy(actions = Seq(addD_P2))
      assert(copied.readFilePathLookup eq info.readFilePathLookup)
      assert(copied.readFilePathToPartitionValues eq lookup)

      // A copy with different read files gets a lookup over its own read files.
      val withNewReadFiles = copied.copy(readFiles = Set(addB_P1))
      assert(withNewReadFiles.readFilePathToPartitionValues ===
        Map(addB_P1.path -> Map("part" -> "1")))
      assert(info.readFilePathToPartitionValues === expected)
      assert(txnInfo(log.update(), readFiles = Set.empty).readFilePathToPartitionValues.isEmpty)
    }
  }

  test("read file lookup does not affect equality, hash code or string form") {
    withLog(Seq(addA_P1)) { log =>
      val info = txnInfo(log.update(), readFiles = Set(addA_P1))
      info.readFilePathToPartitionValues
      val other = info.copy(readFilePathLookup = new ReadFilePathLookup)
      assert(info === other)
      assert(info.hashCode === other.hashCode)
      assert(info.toString === other.toString)
      assert(!info.toString.contains(s"${addA_P1.path} ->"))
    }
  }

  test("detect delete of a read file after several non-conflicting winning commits") {
    withLog(Seq(addA_P1, addB_P1, addC_P2)) { log =>
      val txn = log.startTransaction()
      assert(txn.filterFiles(('part === "1").expr :: Nil).map(_.path).toSet ===
        Set(addA_P1.path, addB_P1.path))

      // Winning commits: a blind append, a delete of a file the txn did not read, and then a
      // delete of a file the txn read.
      log.startTransaction().commit(addE_P3 :: Nil, ManualUpdate)
      log.startTransaction().commit(addC_P2.remove :: Nil, ManualUpdate)
      log.startTransaction().commit(addA_P1.remove :: Nil, ManualUpdate)
      val conflictingVersion = log.update().version

      val e = intercept[ConcurrentDeleteReadException] {
        txn.commit(addB_P1.remove :: addD_P2 :: Nil, ManualUpdate)
      }
      assert(e.getMessage.contains(s"committed at version $conflictingVersion"))
      assert(e.getMessage.contains("partition [part=1]"))
    }
  }

  test("commit after several winning commits that do not delete read files") {
    withLog(Seq(addA_P1, addB_P1, addC_P2)) { log =>
      val txn = log.startTransaction()
      txn.filterFiles(('part === "1").expr :: Nil)

      log.startTransaction().commit(addE_P3 :: Nil, ManualUpdate)
      log.startTransaction().commit(addC_P2.remove :: Nil, ManualUpdate)
      log.startTransaction().commit(addD_P2 :: Nil, ManualUpdate)

      txn.commit(addA_P1.remove :: Nil, ManualUpdate)
      assert(log.update().allFiles.map(_.path).collect().toSet ===
        Set(addB_P1.path, addD_P2.path, addE_P3.path))
    }
  }

  test("detect delete of a file deleted by the txn after several winning commits") {
    withLog(Seq(addA_P1, addC_P2)) { log =>
      val txn = log.startTransaction()
      // Read a partition that is disjoint with all winning commits.
      assert(txn.filterFiles(('part === "4").expr :: Nil).isEmpty)

      log.startTransaction().commit(addE_P3 :: Nil, ManualUpdate)
      log.startTransaction().commit(addC_P2.remove :: Nil, ManualUpdate)
      log.startTransaction().commit(addA_P1.remove :: Nil, ManualUpdate)
      val conflictingVersion = log.update().version

      val e = intercept[ConcurrentDeleteDeleteException] {
        txn.commit(addA_P1.remove :: Nil, ManualUpdate)
      }
      assert(e.getMessage.contains(s"committed at version $conflictingVersion"))
      assert(e.getMessage.contains("partition [part=1]"))
    }
  }
}
