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
import io.delta.standalone.internal.util.{ComparisonUtil, OSSUtil, StandaloneUtil}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.delta.{DeltaLog => OSSDeltaLog}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class OSSCompatibilitySuite extends QueryTest with SharedSparkSession with ComparisonUtil {

  private val now = System.currentTimeMillis()
  private val ss = new StandaloneUtil(now)
  private val oo = new OSSUtil(now)

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

  test("Standalone (with fixed Protocol(1, 2)) read from higher protocol OSS table") {
    // TODO
  }

  test("concurrency conflicts") {
    // TODO
  }
}
