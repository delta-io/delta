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

package org.apache.spark.sql.delta

import java.io.IOException
import java.nio.file.FileAlreadyExistsException
import java.util.concurrent.{Callable, ConcurrentLinkedQueue, TimeUnit}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.actions.{Action, AddFile, Metadata}
import org.apache.spark.sql.delta.hooks.PostCommitHook
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.{DelegatingLogStore, LogStore, LogStoreAdaptor}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import io.delta.storage.integration.S3NativeTestFixture
import io.delta.storage.integration.S3NativeTestFixture._
import org.apache.hadoop.fs.s3a.S3AFileSystem

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

/** Exercises the Spark commit loop through a real S3A client and a local S3 HTTP endpoint. */
class NativeS3CommitSuite
  extends QueryTest
  with DeltaSQLCommandTest
  with SharedSparkSession {

  private val nativeLogStoreClass = "io.delta.storage.S3LogStore"
  private val tableKey = "table"

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(LogStore.logStoreSchemeConfKey("s3a"), nativeLogStoreClass)
  }

  private def commitKey(version: Long): String =
    s"$tableKey/_delta_log/${f"$version%020d"}.json"

  private def requestTrace(fixture: S3NativeTestFixture): String = {
    fixture.requests.map { request =>
      val query = request.query.toSeq.sorted.map { case (key, value) =>
        s"$key=$value"
      }.mkString("&")
      s"${request.operation} ${request.key}?$query " +
        s"status=${request.status} accepted=${request.accepted}"
    }.mkString("\n")
  }

  private def withIdempotencyCheck(enabled: Boolean)(f: => Unit): Unit = {
    val key = DeltaSQLConf.DELTA_COMMIT_IDEMPOTENCY_CHECK_ENABLED.key
    val previous = spark.conf.getOption(key)
    if (enabled) spark.conf.set(key, "true") else spark.conf.unset(key)
    try {
      assert(spark.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_COMMIT_IDEMPOTENCY_CHECK_ENABLED) == enabled)
      if (!enabled) assert(!spark.conf.contains(key), "exercise the unset, default-off flag")
      f
    } finally {
      previous match {
        case Some(value) => spark.conf.set(key, value)
        case None => spark.conf.unset(key)
      }
    }
  }

  private def withNativeTable(
      checkpointInterval: Int = 10)(f: (S3NativeTestFixture, DeltaLog) => Unit): Unit = {
    val fixture = new S3NativeTestFixture
    val conf = fixture.configuration
    // DeltaLog and Spark tasks repeatedly resolve the same URI. Reuse and explicitly close the
    // S3A client so each fixture's endpoint has a bounded client lifetime.
    conf.setBoolean("fs.s3a.impl.disable.cache", false)
    val tablePath = fixture.path(tableKey)
    try {
      val fs = tablePath.getFileSystem(conf)
      try {
        assert(fs.isInstanceOf[S3AFileSystem])
        val options = conf.iterator().asScala.map(e => e.getKey -> e.getValue).toMap
        val log = DeltaLog.forTable(spark, tablePath, options)
        val delegate = log.store.asInstanceOf[DelegatingLogStore].getDelegate(log.logPath)
        assert(delegate.asInstanceOf[LogStoreAdaptor].logStoreImpl.getClass.getName ==
          nativeLogStoreClass)
        val metadata = Metadata(configuration = Map(
          DeltaConfigs.CHECKPOINT_INTERVAL.key -> checkpointInterval.toString))
        assert(log.startTransaction().commit(Seq(metadata), ManualUpdate) == 0)
        f(fixture, log)
        assert(fixture.handlerErrors.isEmpty, fixture.handlerErrors.mkString("\n"))
        assert(fixture.pendingUploads == 0, "multipart uploads must not leak")
      } finally {
        DeltaLog.clearCache()
        fs.close()
      }
    } finally {
      fixture.close()
    }
  }

  private class RecordingHook extends PostCommitHook {
    override val name: String = "record native S3 commit completion"
    private val commits = new ConcurrentLinkedQueue[CommittedTransaction]

    override def run(spark: SparkSession, txn: CommittedTransaction): Unit = {
      commits.add(txn)
    }

    def versions: Seq[Long] = commits.iterator().asScala.map(_.committedVersion).toSeq
  }

  for (idempotencyEnabled <- Seq(false, true)) {
    val flagDescription = if (idempotencyEnabled) "enabled" else "unset (default off)"

    test(s"two native S3 writers resolve a foreign conflict with OCC, flag $flagDescription") {
      withIdempotencyCheck(idempotencyEnabled) {
        withNativeTable() { (fixture, log) =>
          // Both transactions read version 0 before either starts committing.
          val writers = Seq("first-file", "second-file").map { file =>
            val txn = log.startTransaction()
            assert(txn.readVersion == 0)
            assert(!txn.isCommitLockEnabled, "the race must reach S3 without a driver commit lock")
            val hook = new RecordingHook
            txn.registerPostCommitHook(hook)
            (file, txn, hook)
          }
          val gate = fixture.pause(Put, commitKey(1), arrivals = 2)
          val pool = ThreadUtils.newDaemonFixedThreadPool(2, "native-s3-writers")
          try {
            val results = writers.map { case (file, txn, _) =>
              pool.submit(new Callable[Long] {
                override def call(): Long = spark.withActive {
                  txn.commit(Seq(createTestAddFile(encodedPath = file)), ManualUpdate)
                }
              })
            }
            assert(gate.await(), "both writers must reach the same version's conditional PUT")
            gate.close()
            val versions = results.map(_.get(60, TimeUnit.SECONDS))
            assert(versions.sorted == Seq(1L, 2L))
            writers.zip(versions).foreach { case ((file, _, hook), version) =>
              assert(hook.versions == Seq(version))
              val adds = log.store.read(fixture.path(commitKey(version)), log.newDeltaHadoopConf())
                .map(Action.fromJson).collect { case add: AddFile => add.path }
              assert(adds == Seq(file), "each writer must appear only in its committed version")
            }

            val firstVersionPuts = fixture.requests.filter(r =>
              r.operation == Put && r.key == commitKey(1))
            assert(firstVersionPuts.size >= 2)
            assert(firstVersionPuts.forall(_.conditional))
            assert(firstVersionPuts.exists(_.status == 412), "the loser must see an S3 conflict")
            val writeIdentities = firstVersionPuts.map(_.headers.filter { case (key, _) =>
              key.startsWith("x-amz-meta-")
            }).distinct
            assert(writeIdentities.size == 2 && writeIdentities.forall(_.nonEmpty),
              "concurrent writes must carry distinct ownership metadata")
            assert(fixture.acceptedPublications(commitKey(1)) == 1)
            assert(fixture.acceptedPublications(commitKey(2)) == 1)
            assert(fixture.bytes(commitKey(3)).isEmpty)
            val snapshot = log.update()
            assert(snapshot.version == 2)
            assert(snapshot.allFiles.collect().map(_.path).sorted.toSeq ==
              Seq("first-file", "second-file"))
          } finally {
            gate.close()
            pool.shutdownNow()
            assert(pool.awaitTermination(30, TimeUnit.SECONDS), "writers did not stop")
          }
        }
      }
    }

    test(s"native S3 recovers a lost ACK once and runs hooks, flag $flagDescription") {
      withIdempotencyCheck(idempotencyEnabled) {
        withNativeTable(checkpointInterval = 1) { (fixture, log) =>
          val txn = log.startTransaction()
          val hook = new RecordingHook
          txn.registerPostCommitHook(hook)
          fixture.loseNextResponse(Put, commitKey(1))

          assert(txn.commit(Seq(createTestAddFile(encodedPath = "recovered-file")),
            ManualUpdate) == 1)

          val attempts = fixture.requests.filter(r => r.operation == Put && r.key == commitKey(1))
          assert(attempts.nonEmpty && attempts.forall(_.conditional))
          assert(attempts.exists(r => r.accepted && r.status == 0),
            "the fixture must publish the commit and drop its response")
          assert(fixture.requests.exists(r =>
            r.operation == Head && r.key == commitKey(1) && r.status == 200),
            "recovery must identify the landed write through HEAD")
          assert(fixture.acceptedPublications(commitKey(1)) == 1)
          assert(!fixture.requests.exists(r => r.operation == Put && r.key == commitKey(2)),
            "recovery must return success without writing the transaction at a second version")
          assert(fixture.bytes(commitKey(2)).isEmpty)
          assert(hook.versions == Seq(1L))
          val snapshot = log.update()
          assert(snapshot.version == 1)
          assert(snapshot.allFiles.collect().map(_.path).toSeq == Seq("recovered-file"))
          assert(log.readLastCheckpointFile().exists(_.version == 1),
            "the recovered commit must run the checkpoint hook")
          assert(!fixture.requests.exists(r => r.operation == List && r.status >= 400),
            s"fixture LIST failed during checksum publication:\n${requestTrace(fixture)}")
          val checksumKey = s"$tableKey/_delta_log/00000000000000000001.crc"
          assert(fixture.bytes(checksumKey).exists(_.nonEmpty),
            s"the checksum hook must publish a nonempty CRC:\n${requestTrace(fixture)}")
          assert(log.readChecksum(version = 1).isDefined,
            "the recovered commit must run the checksum hook and produce a readable CRC:\n" +
              requestTrace(fixture))
        }
      }
    }

    test(s"native S3 unknown ownership fails without an OCC retry, flag $flagDescription") {
      withIdempotencyCheck(idempotencyEnabled) {
        withNativeTable() { (fixture, log) =>
          val txn = log.startTransaction()
          val hook = new RecordingHook
          txn.registerPostCommitHook(hook)
          fixture.loseNextResponse(Put, commitKey(1))
          fixture.failHeadAfterAcceptance(commitKey(1))

          val error = intercept[IOException] {
            txn.commit(Seq(createTestAddFile(encodedPath = "unknown-file")), ManualUpdate)
          }

          assert(!error.isInstanceOf[FileAlreadyExistsException],
            "unknown ownership must not masquerade as a conflict that OCC can retry")
          assert(fixture.acceptedPublications(commitKey(1)) == 1)
          assert(fixture.bytes(commitKey(1)).nonEmpty,
            "the failed acknowledgement hid a real write")
          assert(fixture.requests.exists(r => r.operation == Put && r.key == commitKey(1) &&
            r.accepted && r.status == 0))
          assert(fixture.requests.exists(r =>
            r.operation == Head && r.key == commitKey(1) && r.status == 503),
            "the recovery HEAD must actually fail")
          assert(!fixture.requests.exists(r => r.operation == Put && r.key == commitKey(2)),
            "unknown outcomes must stop before OCC writes the next version")
          assert(fixture.bytes(commitKey(2)).isEmpty)
          assert(hook.versions.isEmpty, "a failed commit must not run post-commit hooks")
          assert(txn.getCommitted.isEmpty)
          assert(fixture.bytes(s"$tableKey/_delta_log/00000000000000000001.crc").isEmpty)
        }
      }
    }
  }
}
