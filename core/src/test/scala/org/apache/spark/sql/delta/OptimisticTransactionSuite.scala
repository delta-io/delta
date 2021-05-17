/*
 * Copyright (2020) The Delta Lake Project Authors.
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
import org.apache.spark.sql.delta.DeltaTestUtils.OptimisticTxnTestHelper
import org.apache.spark.sql.delta.actions.{Action, AddCDCFile, AddFile, FileAction, Metadata, RemoveFile, SetTransaction}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class OptimisticTransactionSuite extends QueryTest with SharedSparkSession {
  private val addA = AddFile("a", Map.empty, 1, 1, dataChange = true)
  private val addB = AddFile("b", Map.empty, 1, 1, dataChange = true)
  private val addC = AddFile("c", Map.empty, 1, 1, dataChange = true)

  import testImplicits._

  test("block append against metadata change") {
    withTempDir { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
      // Initialize the log.
      log.startTransaction().commitManually()

      val txn = log.startTransaction()
      val winningTxn = log.startTransaction()
      winningTxn.commit(Metadata() :: Nil, ManualUpdate)
      intercept[MetadataChangedException] {
        txn.commit(addA :: Nil, ManualUpdate)
      }
    }
  }

  test("block read+append against append") {
    withTempDir { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
      // Initialize the log.
      log.startTransaction().commitManually()

      val txn = log.startTransaction()
      // reads the table
      txn.filterFiles()
      val winningTxn = log.startTransaction()
      winningTxn.commit(addA :: Nil, ManualUpdate)
      // TODO: intercept a more specific exception
      intercept[DeltaConcurrentModificationException] {
        txn.commit(addB :: Nil, ManualUpdate)
      }
    }
  }

  test("allow blind-append against any data change") {
    withTempDir { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
      // Initialize the log and add data.
      log.startTransaction().commitManually(addA)

      val txn = log.startTransaction()
      val winningTxn = log.startTransaction()
      winningTxn.commit(addA.remove :: addB :: Nil, ManualUpdate)
      txn.commit(addC :: Nil, ManualUpdate)
      checkAnswer(log.update().allFiles.select("path"), Row("b") :: Row("c") :: Nil)
    }
  }

  test("allow read+append+delete against no data change") {
    withTempDir { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
      // Initialize the log and add data. ManualUpdate is just a no-op placeholder.
      log.startTransaction().commitManually(addA)

      val txn = log.startTransaction()
      txn.filterFiles()
      val winningTxn = log.startTransaction()
      winningTxn.commit(Nil, ManualUpdate)
      txn.commit(addA.remove :: addB :: Nil, ManualUpdate)
      checkAnswer(log.update().allFiles.select("path"), Row("b") :: Nil)
    }
  }


  val A_P1 = "part=1/a"
  val B_P1 = "part=1/b"
  val C_P1 = "part=1/c"
  val C_P2 = "part=2/c"
  val D_P2 = "part=2/d"
  val E_P3 = "part=3/e"
  val F_P3 = "part=3/f"
  val G_P4 = "part=4/g"

  private val addA_P1 = AddFile(A_P1, Map("part" -> "1"), 1, 1, dataChange = true)
  private val addB_P1 = AddFile(B_P1, Map("part" -> "1"), 1, 1, dataChange = true)
  private val addC_P1 = AddFile(C_P1, Map("part" -> "1"), 1, 1, dataChange = true)
  private val addC_P2 = AddFile(C_P2, Map("part" -> "2"), 1, 1, dataChange = true)
  private val addD_P2 = AddFile(D_P2, Map("part" -> "2"), 1, 1, dataChange = true)
  private val addE_P3 = AddFile(E_P3, Map("part" -> "3"), 1, 1, dataChange = true)
  private val addF_P3 = AddFile(F_P3, Map("part" -> "3"), 1, 1, dataChange = true)
  private val addG_P4 = AddFile(G_P4, Map("part" -> "4"), 1, 1, dataChange = true)

  test("allow concurrent commit on disjoint partitions") {
    withLog(addA_P1 :: addE_P3 :: Nil) { log =>
      val tx1 = log.startTransaction()
      // TX1 reads P3 (but not P1)
      val tx1Read = tx1.filterFiles(('part === 3).expr :: Nil)
      assert(tx1Read.map(_.path) == E_P3 :: Nil)

      val tx2 = log.startTransaction()
      tx2.filterFiles()
      // TX2 modifies only P1
      tx2.commit(addB_P1 :: Nil, ManualUpdate)

      // free to commit because P1 modified by TX2 was not read
      tx1.commit(addC_P2 :: addE_P3.remove :: Nil, ManualUpdate)
      checkAnswer(
        log.update().allFiles.select("path"),
        Row(A_P1) :: // start (E_P3 was removed by TX1)
        Row(B_P1) :: // TX2
        Row(C_P2) :: Nil) // TX1
    }
  }

  test("allow concurrent commit on disjoint partitions reading all partitions") {
    withLog(addA_P1 :: addD_P2 :: Nil) { log =>
      val tx1 = log.startTransaction()
      // TX1 read P1
      tx1.filterFiles(('part isin 1).expr :: Nil)

      val tx2 = log.startTransaction()
      tx2.filterFiles()
      tx2.commit(addC_P2 :: addD_P2.remove :: Nil, ManualUpdate)

      tx1.commit(addE_P3 :: addF_P3 :: Nil, ManualUpdate)

      checkAnswer(
        log.update().allFiles.select("path"),
        Row(A_P1) :: // start
        Row(C_P2) :: // TX2
        Row(E_P3) :: Row(F_P3) :: Nil) // TX1
    }
  }

  test("block concurrent commit when read partition was appended to by concurrent write") {
    withLog(addA_P1 :: addD_P2 :: addE_P3 :: Nil) { log =>
      val tx1 = log.startTransaction()
      // TX1 reads only P1
      val tx1Read = tx1.filterFiles(('part === 1).expr :: Nil)
      assert(tx1Read.map(_.path) == A_P1 :: Nil)

      val tx2 = log.startTransaction()
      tx2.filterFiles()
      // TX2 modifies only P1
      tx2.commit(addB_P1 :: Nil, ManualUpdate)

      intercept[ConcurrentAppendException] {
        // P1 was modified
        tx1.commit(addC_P2 :: addE_P3 :: Nil, ManualUpdate)
      }
    }
  }

  test("block concurrent commit on full table scan") {
    withLog(addA_P1 :: addD_P2 :: Nil) { log =>
      val tx1 = log.startTransaction()
      // TX1 full table scan
      tx1.filterFiles()
      tx1.filterFiles(('part === 1).expr :: Nil)

      val tx2 = log.startTransaction()
      tx2.filterFiles()
      tx2.commit(addC_P2 :: addD_P2.remove :: Nil, ManualUpdate)

      intercept[ConcurrentAppendException] {
        tx1.commit(addE_P3 :: addF_P3 :: Nil, ManualUpdate)
      }
    }
  }

  val A_1_1 = "a=1/b=1/a"
  val B_1_2 = "a=1/b=2/b"
  val C_2_1 = "a=2/b=1/c"
  val D_3_1 = "a=3/b=1/d"

  val addA_1_1_nested = AddFile(
    A_1_1, Map("a" -> "1", "b" -> "1"),
    1, 1, dataChange = true)
  val addB_1_2_nested = AddFile(
    B_1_2, Map("a" -> "1", "b" -> "2"),
    1, 1, dataChange = true)
  val addC_2_1_nested = AddFile(
    C_2_1, Map("a" -> "2", "b" -> "1"),
    1, 1, dataChange = true)
  val addD_3_1_nested = AddFile(
    D_3_1, Map("a" -> "3", "b" -> "1"),
    1, 1, dataChange = true)

  test("allow concurrent adds to disjoint nested partitions when read is disjoint from write") {
    withLog(addA_1_1_nested :: Nil, partitionCols = "a" :: "b" :: Nil) { log =>
      val tx1 = log.startTransaction()
      // TX1 reads a=1/b=1
      val tx1Read = tx1.filterFiles(('a === 1 and 'b === 1).expr :: Nil)
      assert(tx1Read.map(_.path) == A_1_1 :: Nil)

      val tx2 = log.startTransaction()
      tx2.filterFiles()
      // TX2 reads all partitions and modifies only a=1/b=2
      tx2.commit(addB_1_2_nested :: Nil, ManualUpdate)

      // TX1 reads a=1/b=1 which was not modified by TX2, hence TX1 can write to a=2/b=1
      tx1.commit(addC_2_1_nested :: Nil, ManualUpdate)
      checkAnswer(
        log.update().allFiles.select("path"),
        Row(A_1_1) :: // start
        Row(B_1_2) :: // TX2
        Row(C_2_1) :: Nil) // TX1
    }
  }

  test("allow concurrent adds to same nested partitions when read is disjoint from write") {
    withLog(addA_1_1_nested :: Nil, partitionCols = "a" :: "b" :: Nil) { log =>
      val tx1 = log.startTransaction()
      // TX1 reads a=1/b=1
      val tx1Read = tx1.filterFiles(('a === 1 and 'b === 1).expr :: Nil)
      assert(tx1Read.map(_.path) == A_1_1 :: Nil)

      val tx2 = log.startTransaction()
      tx2.filterFiles()
      // TX2 modifies a=1/b=2
      tx2.commit(addB_1_2_nested :: Nil, ManualUpdate)

      // TX1 reads a=1/b=1 which was not modified by TX2, hence TX1 can write to a=2/b=1
      val add = AddFile(
        "a=1/b=2/x", Map("a" -> "1", "b" -> "2"),
        1, 1, dataChange = true)
      tx1.commit(add :: Nil, ManualUpdate)
      checkAnswer(
        log.update().allFiles.select("path"),
        Row(A_1_1) :: // start
        Row(B_1_2) :: // TX2
        Row("a=1/b=2/x") :: Nil) // TX1
    }
  }

  test("allow concurrent add when read at lvl1 partition is disjoint from concur. write at lvl2") {
    withLog(
      addA_1_1_nested :: addB_1_2_nested :: Nil,
      partitionCols = "a" :: "b" :: Nil) { log =>
      val tx1 = log.startTransaction()
      // TX1 reads a=1
      val tx1Read = tx1.filterFiles(('a === 1).expr :: Nil)
      assert(tx1Read.map(_.path).toSet == Set(A_1_1, B_1_2))

      val tx2 = log.startTransaction()
      tx2.filterFiles()
      // TX2 modifies only a=2/b=1
      tx2.commit(addC_2_1_nested :: Nil, ManualUpdate)

      // free to commit a=2/b=1
      tx1.commit(addD_3_1_nested :: Nil, ManualUpdate)
      checkAnswer(
        log.update().allFiles.select("path"),
        Row(A_1_1) :: Row(B_1_2) :: // start
        Row(C_2_1) ::               // TX2
        Row(D_3_1) :: Nil)          // TX1
    }
  }

  test("block commit when read at lvl1 partition reads lvl2 file concur. deleted") {
    withLog(
      addA_1_1_nested :: addB_1_2_nested :: Nil,
      partitionCols = "a" :: "b" :: Nil) { log =>

      val tx1 = log.startTransaction()
      // TX1 reads a=1
      val tx1Read = tx1.filterFiles(('a === 1).expr :: Nil)
      assert(tx1Read.map(_.path).toSet == Set(A_1_1, B_1_2))

      val tx2 = log.startTransaction()
      tx2.filterFiles()
      // TX2 modifies a=1/b=1
      tx2.commit(addA_1_1_nested.remove :: Nil, ManualUpdate)

      intercept[ConcurrentDeleteReadException] {
        // TX2 modified a=1, which was read by TX1
        tx1.commit(addD_3_1_nested :: Nil, ManualUpdate)
      }
    }
  }

  test("block commit when full table read conflicts with concur. write in lvl2 nested partition") {
    withLog(addA_1_1_nested :: Nil, partitionCols = "a" :: "b" :: Nil) { log =>
      val tx1 = log.startTransaction()
      // TX1 full table scan
      tx1.filterFiles()

      val tx2 = log.startTransaction()
      tx2.filterFiles()
      // TX2 modifies only a=1/b=2
      tx2.commit(addB_1_2_nested :: Nil, ManualUpdate)

      intercept[ConcurrentAppendException] {
        // TX2 modified table all of which was read by TX1
        tx1.commit(addC_2_1_nested :: Nil, ManualUpdate)
      }
    }
  }

  test("block commit when part. range read conflicts with concur. write in lvl2 nested partition") {
    withLog(
      addA_1_1_nested :: Nil,
      partitionCols = "a" :: "b" :: Nil) { log =>

      val tx1 = log.startTransaction()
      // TX1 reads multiple nested partitions a >= 1 or b > 1
      val tx1Read = tx1.filterFiles(('a >= 1 or 'b > 1).expr :: Nil)
      assert(tx1Read.map(_.path).toSet == Set(A_1_1))

      val tx2 = log.startTransaction()
      tx2.filterFiles()
      // TX2 modifies a=1/b=2
      tx2.commit(addB_1_2_nested :: Nil, ManualUpdate)

      intercept[ConcurrentAppendException] {
        // partition a=1/b=2 conflicts with our read a >= 1 or 'b > 1
        tx1.commit(addD_3_1_nested :: Nil, ManualUpdate)
      }
    }
  }

  test("block commit with concurrent removes on same file") {
    withLog(addB_1_2_nested :: Nil, partitionCols = "a" :: "b" :: Nil) { log =>
      val tx1 = log.startTransaction()
      // TX1 reads a=2 so that read is disjoint with write partition.
      tx1.filterFiles(('a === 2).expr :: Nil)

      val tx2 = log.startTransaction()
      tx2.filterFiles()
      // TX2 modifies a=1/b=2
      tx2.commit(addB_1_2_nested.remove :: Nil, ManualUpdate)

      intercept[ConcurrentDeleteDeleteException] {
        // TX1 read does not conflict with TX2 as disjoint partitions
        // But TX2 removed the same file that TX1 is trying to remove
        tx1.commit(addB_1_2_nested.remove:: Nil, ManualUpdate)
      }
    }
  }

  test("block commit when full table read conflicts with add in any partition") {
    withLog(addA_P1 :: Nil) { log =>
      val tx1 = log.startTransaction()
      tx1.filterFiles()

      val tx2 = log.startTransaction()
      tx2.filterFiles()
      tx2.commit(addC_P2.remove :: addB_P1 :: Nil, ManualUpdate)

      intercept[ConcurrentAppendException] {
        // TX1 read whole table but TX2 concurrently modified partition P2
        tx1.commit(addD_P2 :: Nil, ManualUpdate)
      }
    }
  }

  test("block commit when full table read conflicts with delete in any partition") {
    withLog(addA_P1 :: addC_P2 :: Nil) { log =>
      val tx1 = log.startTransaction()
      tx1.filterFiles()

      val tx2 = log.startTransaction()
      tx2.filterFiles()
      tx2.commit(addA_P1.remove :: Nil, ManualUpdate)

      intercept[ConcurrentDeleteReadException] {
        // TX1 read whole table but TX2 concurrently modified partition P1
        tx1.commit(addB_P1.remove :: Nil, ManualUpdate)
      }
    }
  }

  test("block concurrent replaceWhere initial empty") {
    withLog(addA_P1 :: Nil) { log =>
      val tx1 = log.startTransaction()
      // replaceWhere (part >= 2) -> empty read
      val tx1Read = tx1.filterFiles(('part >= 2).expr :: Nil)
      assert(tx1Read.isEmpty)

      val tx2 = log.startTransaction()
      // replaceWhere (part >= 2) -> empty read
      val tx2Read = tx2.filterFiles(('part >= 2).expr :: Nil)
      assert(tx2Read.isEmpty)
      tx2.commit(addE_P3 :: Nil, ManualUpdate)

      intercept[ConcurrentAppendException] {
        // Tx2 have modified P2 which conflicts with our read (part >= 2)
        tx1.commit(addC_P2 :: Nil, ManualUpdate)
      }
    }
  }

  test("allow concurrent replaceWhere disjoint partitions initial empty") {
    withLog(addA_P1 :: Nil) { log =>
      val tx1 = log.startTransaction()
      // replaceWhere (part > 2 and part <= 3) -> empty read
      val tx1Read = tx1.filterFiles(('part > 1 and 'part <= 3).expr :: Nil)
      assert(tx1Read.isEmpty)

      val tx2 = log.startTransaction()
      // replaceWhere (part > 3) -> empty read
      val tx2Read = tx2.filterFiles(('part > 3).expr :: Nil)
      assert(tx2Read.isEmpty)

      tx1.commit(addC_P2 :: Nil, ManualUpdate)
      // P2 doesn't conflict with read predicate (part > 3)
      tx2.commit(addG_P4 :: Nil, ManualUpdate)
      checkAnswer(
        log.update().allFiles.select("path"),
        Row(A_P1) :: // start
        Row(C_P2) :: // TX1
        Row(G_P4) :: Nil) // TX2
    }
  }

  test("block concurrent replaceWhere NOT empty but conflicting predicate") {
    withLog(addA_P1 :: addG_P4 :: Nil) { log =>
      val tx1 = log.startTransaction()
      // replaceWhere (part <= 3) -> read P1
      val tx1Read = tx1.filterFiles(('part <= 3).expr :: Nil)
      assert(tx1Read.map(_.path) == A_P1 :: Nil)
      val tx2 = log.startTransaction()
      // replaceWhere (part >= 2) -> read P4
      val tx2Read = tx2.filterFiles(('part >= 2).expr :: Nil)
      assert(tx2Read.map(_.path) == G_P4 :: Nil)

      tx1.commit(addA_P1.remove :: addC_P2 :: Nil, ManualUpdate)
      intercept[ConcurrentAppendException] {
        // Tx1 have modified P2 which conflicts with our read (part >= 2)
        tx2.commit(addG_P4.remove :: addE_P3 :: Nil, ManualUpdate)
      }
    }
  }

  test("block concurrent commit on read & add conflicting partitions") {
    withLog(addA_P1 :: Nil) { log =>
      val tx1 = log.startTransaction()
      // read P1
      val tx1Read = tx1.filterFiles(('part === 1).expr :: Nil)
      assert(tx1Read.map(_.path) == A_P1 :: Nil)

      // tx2 commits before tx1
      val tx2 = log.startTransaction()
      tx2.filterFiles()
      tx2.commit(addB_P1 :: Nil, ManualUpdate)

      intercept[ConcurrentAppendException] {
        // P1 read by TX1 was modified by TX2
        tx1.commit(addE_P3 :: Nil, ManualUpdate)
      }
    }
  }

  test("block concurrent commit on read & delete conflicting partitions") {
    withLog(addA_P1 :: addB_P1 :: Nil) { log =>
      val tx1 = log.startTransaction()
      // read P1
      tx1.filterFiles(('part === 1).expr :: Nil)

      // tx2 commits before tx1
      val tx2 = log.startTransaction()
      tx2.filterFiles()
      tx2.commit(addA_P1.remove :: Nil, ManualUpdate)

      intercept[ConcurrentDeleteReadException] {
        // P1 read by TX1 was removed by TX2
        tx1.commit(addE_P3 :: Nil, ManualUpdate)
      }
    }
  }

  test("block 2 concurrent replaceWhere transactions") {
    withLog(addA_P1 :: Nil) { log =>
      val tx1 = log.startTransaction()
      // read P1
      tx1.filterFiles(('part === 1).expr :: Nil)

      val tx2 = log.startTransaction()
      // read P1
      tx2.filterFiles(('part === 1).expr :: Nil)

      // tx1 commits before tx2
      tx1.commit(addA_P1.remove :: addB_P1 :: Nil, ManualUpdate)

      intercept[ConcurrentAppendException] {
        // P1 read & deleted by TX1 is being modified by TX2
        tx2.commit(addA_P1.remove :: addC_P1 :: Nil, ManualUpdate)
      }
    }
  }

  test("block 2 concurrent replaceWhere transactions changing partitions") {
    withLog(addA_P1 :: addC_P2 :: addE_P3 :: Nil) { log =>
      val tx1 = log.startTransaction()
      // read P3
      tx1.filterFiles(('part === 3 or 'part === 1).expr :: Nil)

      val tx2 = log.startTransaction()
      // read P3
      tx2.filterFiles(('part === 3 or 'part === 2).expr :: Nil)

      // tx1 commits before tx2
      tx1.commit(addA_P1.remove :: addE_P3.remove :: addB_P1 :: Nil, ManualUpdate)

      intercept[ConcurrentDeleteReadException] {
        // P3 read & deleted by TX1 is being modified by TX2
        tx2.commit(addC_P2.remove :: addE_P3.remove :: addD_P2 :: Nil, ManualUpdate)
      }
    }
  }

  test("block concurrent full table scan after concurrent write completes") {
    withLog(addA_P1 :: addC_P2 :: addE_P3 :: Nil) { log =>
      val tx1 = log.startTransaction()

      val tx2 = log.startTransaction()
      tx2.filterFiles()
      tx2.commit(addC_P2 :: Nil, ManualUpdate)

      tx1.filterFiles(('part === 1).expr :: Nil)
      // full table scan
      tx1.filterFiles()

      intercept[ConcurrentAppendException] {
        tx1.commit(addA_P1.remove :: Nil, ManualUpdate)
      }
    }
  }

  test("block concurrent commit mixed metadata and data predicate") {
    withLog(addA_P1 :: addC_P2 :: addE_P3 :: Nil) { log =>
      val tx1 = log.startTransaction()

      val tx2 = log.startTransaction()
      tx2.filterFiles()
      tx2.commit(addC_P2 :: Nil, ManualUpdate)

      // actually a full table scan
      tx1.filterFiles(('part === 1 or 'year > 2019).expr :: Nil)

      intercept[ConcurrentAppendException] {
        tx1.commit(addA_P1.remove :: Nil, ManualUpdate)
      }
    }
  }

  test("block concurrent read (2 scans) and add when read partition was changed by concur. write") {
    withLog(addA_P1 :: addE_P3 :: Nil) { log =>
      val tx1 = log.startTransaction()
      tx1.filterFiles(('part === 1).expr :: Nil)

      val tx2 = log.startTransaction()
      tx2.filterFiles()
      tx2.commit(addC_P2 :: Nil, ManualUpdate)

      tx1.filterFiles(('part > 1 and 'part < 3).expr :: Nil)

      intercept[ConcurrentAppendException] {
        // P2 added by TX2 conflicts with our read condition 'part > 1 and 'part < 3
        tx1.commit(addA_P1.remove :: Nil, ManualUpdate)
      }
    }
  }

  def setDataChangeFalse(fileActions: Seq[FileAction]): Seq[FileAction] = {
    fileActions.map {
      case a: AddFile => a.copy(dataChange = false)
      case r: RemoveFile => r.copy(dataChange = false)
      case cdc: AddCDCFile => cdc // change files are always dataChange = false
    }
  }

  test("no data change: allow data rearrange when new files concurrently added") {
    withLog(addA_P1 :: addB_P1 :: Nil) { log =>
      val tx1 = log.startTransaction()
      tx1.filterFiles()

      val tx2 = log.startTransaction()
      tx2.filterFiles()
      tx2.commit(
        addE_P3 :: Nil,
        ManualUpdate)

      // tx1 rearranges files
      tx1.commit(
        setDataChangeFalse(addA_P1.remove :: addB_P1.remove :: addC_P1 :: Nil),
        ManualUpdate)

      checkAnswer(
        log.update().allFiles.select("path"),
        Row(C_P1) :: Row(E_P3) ::  Nil)
    }
  }

  test("no data change: block data rearrange when concurrently delete removes same file") {
    withLog(addA_P1 :: addB_P1 :: Nil) { log =>
      val tx1 = log.startTransaction()
      tx1.filterFiles()

      // tx2 removes file
      val tx2 = log.startTransaction()
      tx2.filterFiles()
      tx2.commit(addA_P1.remove :: Nil, ManualUpdate)

      intercept[ConcurrentDeleteReadException] {
        // tx1 reads to rearrange the same file that tx2 deleted
        tx1.commit(
          setDataChangeFalse(addA_P1.remove :: addB_P1.remove :: addC_P1 :: Nil),
          ManualUpdate)
      }
    }
  }

  test("readWholeTable should block concurrent delete") {
    withLog(addA_P1 :: Nil) { log =>
      val tx1 = log.startTransaction()
      tx1.readWholeTable()

      // tx2 removes file
      val tx2 = log.startTransaction()
      tx2.commit(addA_P1.remove :: Nil, ManualUpdate)

      intercept[ConcurrentDeleteReadException] {
        // tx1 reads the whole table but tx2 removes files before tx1 commits
        tx1.commit(addB_P1 :: Nil, ManualUpdate)
      }
    }
  }

  def withLog(
      actions: Seq[Action],
      partitionCols: Seq[String] = "part" :: Nil)(
      test: DeltaLog => Unit): Unit = {

    val schema = StructType(partitionCols.map(p => StructField(p, StringType)).toArray)
    val actionWithMetaData =
      actions :+ Metadata(partitionColumns = partitionCols, schemaString = schema.json)

    withTempDir { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
      // Initialize the log and add data. ManualUpdate is just a no-op placeholder.
      log.startTransaction().commit(Seq(Metadata(partitionColumns = partitionCols)), ManualUpdate)
      log.startTransaction().commitManually(actionWithMetaData: _*)
      test(log)
    }
  }

  test("allow concurrent set-txns with different app ids") {
    withTempDir { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
      // Initialize the log.
      log.startTransaction().commitManually()

      val txn = log.startTransaction()
      txn.txnVersion("t1")
      val winningTxn = log.startTransaction()
      winningTxn.commit(SetTransaction("t2", 1, Some(1234L)) :: Nil, ManualUpdate)
      txn.commit(Nil, ManualUpdate)

      assert(log.update().transactions === Map("t2" -> 1))
    }
  }

  test("block concurrent set-txns with the same app id") {
    withTempDir { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
      // Initialize the log.
      log.startTransaction().commitManually()

      val txn = log.startTransaction()
      txn.txnVersion("t1")
      val winningTxn = log.startTransaction()
      winningTxn.commit(SetTransaction("t1", 1, Some(1234L)) :: Nil, ManualUpdate)

      intercept[ConcurrentTransactionException] {
        txn.commit(Nil, ManualUpdate)
      }
    }
  }

  test("initial commit without metadata should fail") {
    withTempDir { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
      val txn = log.startTransaction()
      withSQLConf(DeltaSQLConf.DELTA_COMMIT_VALIDATION_ENABLED.key -> "true") {
        val e = intercept[IllegalStateException] {
          txn.commit(Nil, ManualUpdate)
        }
        assert(e.getMessage === DeltaErrors.metadataAbsentException().getMessage)
      }

      // Try with commit validation turned off
      withSQLConf(DeltaSQLConf.DELTA_STATE_RECONSTRUCTION_VALIDATION_ENABLED.key -> "false",
          DeltaSQLConf.DELTA_COMMIT_VALIDATION_ENABLED.key -> "false") {
        txn.commit(Nil, ManualUpdate)
        assert(log.update().version === 0)
      }
    }
  }

  test("initial commit with multiple metadata actions should fail") {
    withTempDir { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getAbsolutePath))
      val txn = log.startTransaction()
      val e = intercept[AssertionError] {
        txn.commit(Seq(Metadata(), Metadata()), ManualUpdate)
      }
      assert(e.getMessage.contains("Cannot change the metadata more than once in a transaction."))
    }
  }

  test("AddFile with different partition schema compared to metadata should fail") {
    withTempDir { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getAbsolutePath))
      log.startTransaction().commit(Seq(Metadata(partitionColumns = Seq("col2"))), ManualUpdate)
      withSQLConf(DeltaSQLConf.DELTA_COMMIT_VALIDATION_ENABLED.key -> "true") {
        val e = intercept[IllegalStateException] {
          log.startTransaction().commit(Seq(AddFile(
            log.dataPath.toString, Map("col3" -> "1"), 12322, 0L, true, null, null)), ManualUpdate)
        }
        assert(e.getMessage === DeltaErrors.addFilePartitioningMismatchException(
          Seq("col3"), Seq("col2")).getMessage)
      }
      // Try with commit validation turned off
      withSQLConf(DeltaSQLConf.DELTA_STATE_RECONSTRUCTION_VALIDATION_ENABLED.key -> "false",
        DeltaSQLConf.DELTA_COMMIT_VALIDATION_ENABLED.key -> "false") {
        log.startTransaction().commit(Seq(AddFile(
          log.dataPath.toString, Map("col3" -> "1"), 12322, 0L, true, null, null)), ManualUpdate)
        assert(log.update().version === 1)
      }
    }
  }
}
