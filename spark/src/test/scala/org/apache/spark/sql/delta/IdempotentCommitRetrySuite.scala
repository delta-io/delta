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

import java.nio.file.FileAlreadyExistsException

import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.LocalLogStore
import org.apache.spark.sql.delta.storage.LogStore.logStoreClassConfKey
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class IdempotentCommitRetrySuite
  extends QueryTest
  with DeltaSQLCommandTest
  with SharedSparkSession {

  protected override def sparkConf: SparkConf = {
    super.sparkConf
      .set(logStoreClassConfKey, classOf[FailAfterWriteLogStore].getName)
      .set(DeltaSQLConf.DELTA_COMMIT_IDEMPOTENCY_CHECK_ENABLED.key, "true")
  }

  test("idempotent self-commit is detected and returns success") {
    withTempDir { tempDir =>
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      // Commit the metadata at version 0 (this write is allowed to succeed).
      log.startTransaction(catalogTableOpt = None).commit(Seq(Metadata()), ManualUpdate)

      // The next commit's write lands at version 1 but the response is "lost": the log store
      // throws FileAlreadyExistsException after writing, so the retry loop runs conflict checking
      // and finds this transaction's own commit at version 1.
      FailAfterWriteLogStore.failAtVersion = 1
      // The self-commit fires the detection exactly once and does not rebase to a second version.
      log.startTransaction(catalogTableOpt = None).commit(
        Seq(createTestAddFile(encodedPath = "file-1")), ManualUpdate)

      val snapshot = log.update()
      assert(snapshot.version == 1)
      assert(snapshot.allFiles.collect().map(_.path).toSeq == Seq("file-1"),
        "The AddFile must appear exactly once. No duplication on the idempotent retry")
    }
  }

  test("idempotency check disabled (default): self-commit is not detected") {
    withSQLConf(DeltaSQLConf.DELTA_COMMIT_IDEMPOTENCY_CHECK_ENABLED.key -> "false") {
      withTempDir { tempDir =>
        val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        log.startTransaction(catalogTableOpt = None).commit(Seq(Metadata()), ManualUpdate)

        FailAfterWriteLogStore.failAtVersion = 1
        // With the flag off, the idempotency short-circuit must not run.
        log.startTransaction(catalogTableOpt = None).commit(
          Seq(createTestAddFile(encodedPath = "file-1")), ManualUpdate)

        // The retry committed again and so the table is now at version 2.
        assert(log.update().version == 2)
      }
    }
  }

  test("post-commit hooks run on the idempotent path (checkpoint is written)") {
    withTempDir { tempDir =>
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      // Set the checkpoint interval to 1 so the idempotent commit at version 1
      // triggers a checkpoint.
      log.startTransaction(catalogTableOpt = None).commit(
        Seq(Metadata(configuration = Map(DeltaConfigs.CHECKPOINT_INTERVAL.key -> "1"))),
        ManualUpdate)

      FailAfterWriteLogStore.failAtVersion = 1
      // The self-commit fires the detection exactly once.
      log.startTransaction(catalogTableOpt = None).commit(
        Seq(createTestAddFile(encodedPath = "file-1")), ManualUpdate)

      // The commit landed at version 1 and, because needsCheckpoint was set on the idempotent
      // path, CheckpointHook wrote a checkpoint at version 1.
      assert(log.update().version == 1)
      assert(log.readLastCheckpointFile().exists(_.version == 1),
        "a checkpoint should be written at version 1 on the idempotent path")
      // ChecksumHook also runs on the idempotent path and writes the .crc for the committed
      // version (synchronously, since the checksum threadpool is set to size 0 in this suite).
      assert(log.readChecksum(version = 1).isDefined,
        "a checksum (.crc) should be written at version 1 on the idempotent path")
    }
  }
}

object FailAfterWriteLogStore {

  var failAtVersion: Long = -1

  var blockAfterWrite: Boolean = false

  @volatile var blockedAtWrite: Boolean = false

  @volatile private var block = new java.util.concurrent.CountDownLatch(1)

  def resetBlock(): Unit = block = new java.util.concurrent.CountDownLatch(1)

  def releaseBlock(): Unit = block.countDown()

  def awaitRelease(): Unit = block.await()
}

/**
 * A log store that, for the configured delta version, performs the real write and then throws
 * [[FileAlreadyExistsException]] exactly once, modelling a commit that landed but whose response
 * was lost. When [[FailAfterWriteLogStore.blockAfterWrite]] is set, it blocks after writing (before
 * throwing) until released, so a concurrent writer can advance the table first. All other writes
 * behave normally.
 */
class FailAfterWriteLogStore(sparkConf: SparkConf, defaultHadoopConf: Configuration)
  extends LocalLogStore(sparkConf, defaultHadoopConf) {

  override def write(
      path: Path,
      actions: Iterator[String],
      overwrite: Boolean,
      hadoopConf: Configuration): Unit = {
    val failVersion = FailAfterWriteLogStore.failAtVersion
    val shouldFail =
      failVersion >= 0 && path.getName == f"$failVersion%020d.json"
    // Perform the write so the commit fully lands before we simulate the lost response.
    super.write(path, actions, overwrite, hadoopConf)
    if (shouldFail) {
      // Only fail the first write of this version; the retry (after conflict resolution) must
      // not throw again.
      FailAfterWriteLogStore.failAtVersion = -1
      if (FailAfterWriteLogStore.blockAfterWrite) {
        FailAfterWriteLogStore.blockAfterWrite = false
        FailAfterWriteLogStore.blockedAtWrite = true
        FailAfterWriteLogStore.awaitRelease()
      }
      throw new FileAlreadyExistsException(path.toString)
    }
  }
}
