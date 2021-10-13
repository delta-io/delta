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

package io.delta.standalone.internal.compatibility.tests

import java.io.File
import java.nio.file.Files
import java.util.UUID

import scala.collection.JavaConverters._

import io.delta.standalone.{DeltaLog => StandaloneDeltaLog}
import io.delta.standalone.internal.{DeltaLogImpl => InternalStandaloneDeltaLog}
import io.delta.standalone.internal.util.ComparisonUtil

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.delta.{DeltaLog => OSSDeltaLog}

class OSSCompatibilitySuite extends OssCompatibilitySuiteBase with ComparisonUtil {

  /**
   * Creates a temporary directory, a public Standalone DeltaLog, an internal Standalone DeltaLog,
   * and a DeltaOSS DeltaLog, which are all then passed to `f`.
   *
   * The internal Standalone DeltaLog is used to gain access to internal, non-public Java APIs
   * to verify internal state.
   *
   * The temporary directory will be deleted after `f` returns.
   */
  private def withTempDirAndLogs(
      f: (File, StandaloneDeltaLog, InternalStandaloneDeltaLog, OSSDeltaLog) => Unit): Unit = {
    val dir = Files.createTempDirectory(UUID.randomUUID().toString).toFile

    val standaloneLog = StandaloneDeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
    val standaloneInternalLog =
      InternalStandaloneDeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
    val ossLog = OSSDeltaLog.forTable(spark, dir.getCanonicalPath)

    try f(dir, standaloneLog, standaloneInternalLog, ossLog) finally {
      FileUtils.deleteDirectory(dir)
    }
  }

  test("assert static actions are the same (without any writes/reads)") {
    compareMetadata(ss.metadata, oo.metadata)
    compareAddFiles(ss.addFiles, oo.addFiles)
    compareRemoveFiles(ss.removeFiles, oo.removeFiles)
    compareSetTransaction(ss.setTransaction, oo.setTransaction)
  }

  /**
   * For each (logType1, logType2, action) below, we will test the case of:
   * logType1 write action (A1), logType2 read action (A2), assert A1 == A2
   *
   * case 1a: standalone, oss, Metadata
   * case 1b: oss, standalone, Metadata
   *
   * case 2a: standalone, oss, CommitInfo
   * case 2b: oss, standalone, CommitInfo
   *
   * case 3a: standalone, oss, Protocol
   * case 3b: oss, standalone, Protocol
   *
   * case 4a: standalone, oss, AddFile
   * case 4b: oss, standalone, AddFile
   *
   * case 5a: standalone, oss, RemoveFile
   * case 5b: oss, standalone, RemoveFile
   *
   * case 6a: standalone, oss, SetTransaction
   * case 6b: oss, standalone, SetTransaction
   */
  test("read/write actions") {
    withTempDirAndLogs { (_, standaloneLog, standaloneInternalLog, ossLog) =>
      // === Standalone commit Metadata & CommitInfo ===
      val standaloneTxn0 = standaloneLog.startTransaction()
      standaloneTxn0.commit(Iterable(ss.metadata).asJava, ss.op, ss.engineInfo)

      // case 1a
      compareMetadata(standaloneLog.update().getMetadata, ossLog.update().metadata)

      // case 2a
      compareCommitInfo(standaloneLog.getCommitInfoAt(0), oo.getCommitInfoAt(ossLog, 0))

      // case 3a
      compareProtocol(standaloneInternalLog.update().protocol, ossLog.snapshot.protocol)

      // === OSS commit Metadata & CommitInfo ===
      val ossTxn1 = ossLog.startTransaction()
      ossTxn1.commit(oo.metadata :: Nil, oo.op)

      // case 1b
      compareMetadata(standaloneLog.update().getMetadata, ossLog.update().metadata)

      // case 2b
      compareCommitInfo(standaloneLog.getCommitInfoAt(1), oo.getCommitInfoAt(ossLog, 1))

      // case 3b
      compareProtocol(standaloneInternalLog.update().protocol, ossLog.snapshot.protocol)

      // === Standalone commit AddFiles ===
      val standaloneTxn2 = standaloneLog.startTransaction()
      standaloneTxn2.commit(ss.addFiles.asJava, ss.op, ss.engineInfo)

      def assertAddFiles(): Unit = {
        standaloneLog.update()
        ossLog.update()

        val scanFiles = standaloneLog.snapshot().scan().getFiles.asScala.toSeq
        assert(standaloneLog.snapshot().getAllFiles.size() == ss.addFiles.size)
        assert(scanFiles.size == ss.addFiles.size)
        assert(ossLog.snapshot.allFiles.count() == ss.addFiles.size)

        compareAddFiles(
          standaloneLog.update().getAllFiles.asScala, ossLog.update().allFiles.collect())
        compareAddFiles(scanFiles, ossLog.update().allFiles.collect())
      }

      // case 4a
      assertAddFiles()

      // === OSS commit AddFiles ===
      val ossTxn3 = ossLog.startTransaction()
      ossTxn3.commit(oo.addFiles, oo.op)

      // case 4b
      assertAddFiles()

      // === Standalone commit RemoveFiles ===
      val standaloneTxn4 = standaloneLog.startTransaction()
      standaloneTxn4.commit(ss.removeFiles.asJava, ss.op, ss.engineInfo)

      def assertRemoveFiles(): Unit = {
        standaloneLog.update()
        standaloneInternalLog.update()
        ossLog.update()

        assert(standaloneLog.snapshot().getAllFiles.isEmpty)
        assert(ossLog.snapshot.allFiles.isEmpty)
        assert(standaloneInternalLog.snapshot.tombstones.size == ss.removeFiles.size)
        assert(ossLog.snapshot.tombstones.count() == ss.removeFiles.size)
        compareRemoveFiles(
          standaloneInternalLog.snapshot.tombstones, ossLog.snapshot.tombstones.collect())
      }

      // case 5a
      assertRemoveFiles()

      // === OSS commit RemoveFiles ===
      val ossTxn5 = ossLog.startTransaction()
      ossTxn5.commit(oo.removeFiles, oo.op)

      // case 5b
      assertRemoveFiles()

      // === Standalone commit SetTransaction ===
      val standaloneTxn6 = standaloneLog.startTransaction()
      standaloneTxn6.commit(Iterable(ss.setTransaction).asJava, ss.op, ss.engineInfo)

      def assertSetTransactions(): Unit = {
        standaloneInternalLog.update()
        ossLog.update()
        assert(standaloneInternalLog.snapshot.setTransactionsScala.length == 1)
        assert(ossLog.snapshot.setTransactions.length == 1)
        compareSetTransaction(
          standaloneInternalLog.snapshot.setTransactions.head,
          ossLog.snapshot.setTransactions.head)
      }

      // case 6a
      assertSetTransactions()

      // === OSS commit SetTransaction ===
      val ossTxn7 = ossLog.startTransaction()
      ossTxn7.commit(oo.setTransaction :: Nil, oo.op)

      // case 6b
      assertSetTransactions()
    }
  }

  test("Standalone writer write to higher protocol OSS table should fail") {
    withTempDirAndLogs { (_, standaloneLog, _, ossLog) =>
      ossLog.startTransaction().commit(oo.metadata :: oo.protocol13 :: Nil, oo.op)

      // scalastyle:off line.size.limit
      val e = intercept[io.delta.standalone.internal.exception.DeltaErrors.InvalidProtocolVersionException] {
        // scalastyle:on line.size.limit
        standaloneLog.startTransaction().commit(Iterable().asJava, ss.op, ss.engineInfo)
      }

      assert(e.getMessage.contains(
        """
          |Delta protocol version (1,3) is too new for this version of Delta
          |Standalone Reader/Writer (1,2). Please upgrade to a newer release.
          |""".stripMargin))
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Allowed concurrent actions
  ///////////////////////////////////////////////////////////////////////////

  checkStandalone(
    "append / append",
    conflicts = false,
    reads = Seq(t => t.metadata()),
    concurrentOSSWrites = Seq(oo.conflict.addA),
    actions = Seq(ss.conflict.addB))

  checkOSS(
    "append / append",
    conflicts = false,
    reads = Seq(t => t.metadata),
    concurrentStandaloneWrites = Seq(ss.conflict.addA),
    actions = Seq(oo.conflict.addB))

  checkStandalone(
    "disjoint txns",
    conflicts = false,
    reads = Seq(t => t.txnVersion("foo")),
    concurrentOSSWrites = Seq(oo.setTransaction),
    actions = Nil)

  checkOSS(
    "disjoint txns",
    conflicts = false,
    reads = Seq(t => t.txnVersion("foo")),
    concurrentStandaloneWrites = Seq(ss.setTransaction),
    actions = Nil)

  checkStandalone(
    "disjoint delete / read",
    conflicts = false,
    setup = Seq(ss.conflict.metadata_partX, ss.conflict.addA_partX2),
    reads = Seq(t => t.markFilesAsRead(ss.conflict.colXEq1Filter)),
    concurrentOSSWrites = Seq(oo.conflict.removeA),
    actions = Seq()
  )

  checkOSS(
    "disjoint delete / read",
    conflicts = false,
    setup = Seq(oo.conflict.metadata_partX, oo.conflict.addA_partX2),
    reads = Seq(t => t.filterFiles(oo.conflict.colXEq1Filter :: Nil)),
    concurrentStandaloneWrites = Seq(ss.conflict.removeA),
    actions = Seq()
  )

  checkStandalone(
    "disjoint add / read",
    conflicts = false,
    setup = Seq(ss.conflict.metadata_partX),
    reads = Seq(t => t.markFilesAsRead(ss.conflict.colXEq1Filter)),
    concurrentOSSWrites = Seq(oo.conflict.addA_partX2),
    actions = Seq()
  )

  checkOSS(
    "disjoint add / read",
    conflicts = false,
    setup = Seq(oo.conflict.metadata_partX),
    reads = Seq(t => t.filterFiles(oo.conflict.colXEq1Filter :: Nil)),
    concurrentStandaloneWrites = Seq(ss.conflict.addA_partX2),
    actions = Seq()
  )

  checkStandalone(
    "add / read + no write",  // no write = no real conflicting change even though data was added
    conflicts = false,        // so this should not conflict
    setup = Seq(ss.conflict.metadata_partX),
    reads = Seq(t => t.markFilesAsRead(ss.conflict.colXEq1Filter)),
    concurrentOSSWrites = Seq(oo.conflict.addA_partX1),
    actions = Seq())

  checkOSS(
    "add / read + no write",  // no write = no real conflicting change even though data was added
    conflicts = false,        // so this should not conflict
    setup = Seq(oo.conflict.metadata_partX),
    reads = Seq(t => t.filterFiles(oo.conflict.colXEq1Filter :: Nil)),
    concurrentStandaloneWrites = Seq(ss.conflict.addA_partX1),
    actions = Seq())

  ///////////////////////////////////////////////////////////////////////////
  // Disallowed concurrent actions
  ///////////////////////////////////////////////////////////////////////////

  checkStandalone(
    "delete / delete",
    conflicts = true,
    reads = Nil,
    concurrentOSSWrites = Seq(oo.conflict.removeA),
    actions = Seq(ss.conflict.removeA_time5)
  )

  checkOSS(
    "delete / delete",
    conflicts = true,
    reads = Nil,
    concurrentStandaloneWrites = Seq(ss.conflict.removeA),
    actions = Seq(oo.conflict.removeA_time5)
  )

  checkStandalone(
    "add / read + write",
    conflicts = true,
    setup = Seq(ss.conflict.metadata_partX),
    reads = Seq(t => t.markFilesAsRead(ss.conflict.colXEq1Filter)),
    concurrentOSSWrites = Seq(oo.conflict.addA_partX1),
    actions = Seq(ss.conflict.addB_partX1),
    // commit info should show operation as "Manual Update", because that's the operation used by
    // the harness
    errorMessageHint = Some("[x=1]" :: "Manual Update" :: Nil))

  checkOSS(
    "add / read + write",
    conflicts = true,
    setup = Seq(oo.conflict.metadata_partX),
    reads = Seq(t => t.filterFiles(oo.conflict.colXEq1Filter :: Nil)),
    concurrentStandaloneWrites = Seq(ss.conflict.addA_partX1),
    actions = Seq(oo.conflict.addB_partX1),
    // commit info should show operation as "Manual Update", because that's the operation used by
    // the harness
    errorMessageHint = Some("[x=1]" :: "Manual Update" :: Nil))

  checkStandalone(
    "delete / read",
    conflicts = true,
    setup = Seq(ss.conflict.metadata_partX, ss.conflict.addA_partX1),
    reads = Seq(t => t.markFilesAsRead(ss.conflict.colXEq1Filter)),
    concurrentOSSWrites = Seq(oo.conflict.removeA),
    actions = Seq(),
    errorMessageHint = Some("a in partition [x=1]" :: "Manual Update" :: Nil))

  checkOSS(
    "delete / read",
    conflicts = true,
    setup = Seq(oo.conflict.metadata_partX, oo.conflict.addA_partX1),
    reads = Seq(t => t.filterFiles(oo.conflict.colXEq1Filter :: Nil)),
    concurrentStandaloneWrites = Seq(ss.conflict.removeA),
    actions = Seq(),
    errorMessageHint = Some("a in partition [x=1]" :: "Manual Update" :: Nil))

  checkStandalone(
    "schema change",
    conflicts = true,
    reads = Seq(t => t.metadata),
    concurrentOSSWrites = Seq(oo.metadata),
    actions = Nil)

  checkOSS(
    "schema change",
    conflicts = true,
    reads = Seq(t => t.metadata),
    concurrentStandaloneWrites = Seq(ss.metadata),
    actions = Nil)

  checkStandalone(
    "conflicting txns",
    conflicts = true,
    reads = Seq(t => t.txnVersion(oo.setTransaction.appId)),
    concurrentOSSWrites = Seq(oo.setTransaction),
    actions = Nil)

  checkOSS(
    "conflicting txns",
    conflicts = true,
    reads = Seq(t => t.txnVersion(ss.setTransaction.getAppId)),
    concurrentStandaloneWrites = Seq(ss.setTransaction),
    actions = Nil)

  checkStandalone(
    "upgrade / upgrade",
    conflicts = true,
    reads = Seq(t => t.metadata),
    concurrentOSSWrites = Seq(oo.protocol12),
    actions = Seq(ss.protocol12))

  checkOSS(
    "upgrade / upgrade",
    conflicts = true,
    reads = Seq(t => t.metadata),
    concurrentStandaloneWrites = Seq(ss.protocol12),
    actions = Seq(oo.protocol12))

  checkStandalone(
    "taint whole table",
    conflicts = true,
    setup = Seq(ss.conflict.metadata_partX, ss.conflict.addA_partX2),
    reads = Seq(
      t => t.markFilesAsRead(ss.conflict.colXEq1Filter),
      // `readWholeTable` should disallow any concurrent change, even if the change
      // is disjoint with the earlier filter
      t => t.readWholeTable()
    ),
    concurrentOSSWrites = Seq(oo.conflict.addB_partX3),
    actions = Seq(ss.conflict.addC_partX4)
  )

  checkOSS(
    "taint whole table",
    conflicts = true,
    setup = Seq(oo.conflict.metadata_partX, oo.conflict.addA_partX2),
    reads = Seq(
      t => t.filterFiles(oo.conflict.colXEq1Filter :: Nil),
      // `readWholeTable` should disallow any concurrent change, even if the change
      // is disjoint with the earlier filter
      t => t.readWholeTable()
    ),
    concurrentStandaloneWrites = Seq(ss.conflict.addB_partX3),
    actions = Seq(oo.conflict.addC_partX4)
  )

  checkStandalone(
    "taint whole table + concurrent remove",
    conflicts = true,
    setup = Seq(ss.conflict.metadata_colX, ss.conflict.addA),
    reads = Seq(
      // `readWholeTable` should disallow any concurrent `RemoveFile`s.
      t => t.readWholeTable()
    ),
    concurrentOSSWrites = Seq(oo.conflict.removeA),
    actions = Seq(ss.conflict.addB))

  checkOSS(
    "taint whole table + concurrent remove",
    conflicts = true,
    setup = Seq(oo.conflict.metadata_colX, oo.conflict.addA),
    reads = Seq(
      // `readWholeTable` should disallow any concurrent `RemoveFile`s.
      t => t.readWholeTable()
    ),
    concurrentStandaloneWrites = Seq(ss.conflict.removeA),
    actions = Seq(oo.conflict.addB))
}
