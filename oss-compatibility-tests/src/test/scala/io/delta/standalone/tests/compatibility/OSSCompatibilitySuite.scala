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

package io.delta.standalone.tests.compatibility

import java.io.File
import java.nio.file.Files
import java.util.UUID

import scala.collection.JavaConverters._

import io.delta.standalone.{DeltaLog => StandaloneDeltaLog}
import io.delta.standalone.internal.util.StandaloneUtil
import io.delta.standalone.util.{ComparisonUtil, OSSUtil}

import org.apache.spark.sql.delta.{DeltaLog => OSSDeltaLog}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class OSSCompatibilitySuite extends QueryTest with SharedSparkSession with ComparisonUtil {

  private val ss = StandaloneUtil
  private val oo = OSSUtil

  /**
   * Creates a temporary directory, a Standalone DeltaLog, and a DeltaOSS DeltaLog, which are all
   * then passed to `f`. The temporary directory will be deleted after `f` returns.
   */
  private def withTempDirAndLogs(f: (File, StandaloneDeltaLog, OSSDeltaLog) => Unit): Unit = {
    val dir = Files.createTempDirectory(UUID.randomUUID().toString).toFile

    val standaloneLog = StandaloneDeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
    val ossLog = OSSDeltaLog.forTable(spark, dir.getCanonicalPath)

    try f(dir, standaloneLog, ossLog) finally {
      FileUtils.deleteDirectory(dir)
    }
  }

  test("assert static actions are the same (without any writes/reads)") {
    compareMetadata(ss.metadata, oo.metadata)
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
   * case 3a: standalone, oss, AddFile
   * case 3b: oss, standalone, AddFile
   *
   * case 4a: standalone, oss, RemoveFile
   * case 4b: oss, standalone, RemoveFile
   */
  test("read/write actions") {
    withTempDirAndLogs { (_, standaloneLog, ossLog) =>
      val standaloneTxn0 = standaloneLog.startTransaction()
      standaloneTxn0.commit(Iterable(ss.metadata).asJava, ss.op, ss.engineInfo)

      // case 1a
      compareMetadata(standaloneLog.update().getMetadata, ossLog.update().metadata)

      // case 2a
      compareCommitInfo(standaloneLog.getCommitInfoAt(0), oo.getCommitInfoAt(ossLog, 0))

      val ossTxn1 = ossLog.startTransaction()
      ossTxn1.commit(Seq(oo.metadata), oo.op)

      // case 1b
      compareMetadata(standaloneLog.update().getMetadata, ossLog.update().metadata)

      // case 2b
      compareCommitInfo(standaloneLog.getCommitInfoAt(1), oo.getCommitInfoAt(ossLog, 1))

      val standaloneTxn2 = standaloneLog.startTransaction()
      standaloneTxn2.commit(ss.addFiles.asJava, ss.op, ss.engineInfo)

      // case 3a
      compareAddFiles(standaloneLog.update(), ossLog.update())

      val ossTxn3 = ossLog.startTransaction()
      ossTxn3.commit(oo.addFiles, oo.op)

      // case 3b
      compareAddFiles(standaloneLog.update(), ossLog.update())

      val standaloneTxn4 = standaloneLog.startTransaction()
      standaloneTxn4.commit(ss.removeFiles.asJava, ss.op, ss.engineInfo)

      // case 4a TODO

    }
  }

  test("concurrency conflicts") {
    withTempDirAndLogs { (dir, standaloneLog, ossLog) =>
      // TODO
    }
  }
}
