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
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.FileAlreadyExistsException
import java.util.concurrent.{Callable, ConcurrentHashMap, CountDownLatch, Future, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.util.Try

import org.apache.spark.sql.delta.storage.{DelegatingLogStore, LogStore, LogStoreAdaptor}
import io.delta.storage.{S3LogStore, S3SingleDriverLogStore}
import io.delta.storage.integration.{S3NativeTestFixture, S3NativeTrackedFileSystem}
import io.delta.storage.integration.S3NativeTestFixture._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

/** Same-JVM native/legacy races through real S3A clients and a loopback S3 HTTP endpoint. */
class SharedS3LogStoreLockSuite extends QueryTest with SharedSparkSession with Eventually {
  private val key = "table/_delta_log/00000000000000000000.json"
  private val otherKey = "table/_delta_log/00000000000000000001.json"

  private def configuration(fixture: S3NativeTestFixture, native: Boolean): Configuration = {
    val conf = fixture.configuration
    conf.setBoolean("fs.s3a.impl.disable.cache", true)
    conf.setBoolean("fs.s3a.create.conditional.enabled", native)
    conf
  }

  private def store(fixture: S3NativeTestFixture, conf: Configuration,
      native: Boolean): DelegatingLogStore = {
    assert(!spark.sparkContext.getConf.contains(LogStore.logStoreSchemeConfKey("s3a")))
    // Deliberately supply the endpoint and capability only through each write's configuration.
    val result = new DelegatingLogStore(new Configuration(false))
    val delegate = result.getDelegate(fixture.path(key), conf)
      .asInstanceOf[LogStoreAdaptor].logStoreImpl
    assert(delegate.getClass == (if (native) classOf[S3LogStore]
      else classOf[S3SingleDriverLogStore]))
    result
  }

  private def waitForAttempt(result: Future[_], thread: AtomicReference[Thread]): Unit = {
    // This only synchronizes the race. The assertions below check publication and write results.
    // Old code lets the waiter finish; fixed code blocks it in the shared path lock.
    eventually(timeout(Span(30, Seconds))) {
      val waitingForPathLock = Option(thread.get()).exists { worker =>
        worker.getState == Thread.State.WAITING && worker.getStackTrace.exists { frame =>
          frame.getClassName == "io.delta.storage.internal.PathLock" &&
            frame.getMethodName == "acquire"
        }
      }
      assert(result.isDone || waitingForPathLock,
        "waiter must finish or reach the owner's path lock")
    }
  }

  private def assertContents(fixture: S3NativeTestFixture, objectKey: String,
      line: String, publications: Int): Unit = {
    assert(fixture.bytes(objectKey).exists(_.sameElements((line + "\n").getBytes(UTF_8))),
      s"unexpected bytes at $objectKey")
    assert(fixture.acceptedPublications(objectKey) == publications)
  }

  for {
    nativeFirst <- Seq(true, false)
    (operation, size) <- Seq(Put -> 0, Complete -> (6 * 1024 * 1024))
  } {
    val first = if (nativeFirst) "native" else "legacy"
    test(s"$first owner excludes the other delegate for $operation on the same path") {
      val fixture = new S3NativeTestFixture
      val pool = ThreadUtils.newDaemonFixedThreadPool(3, "shared-s3-path-lock")
      val ownerReady = new CountDownLatch(1)
      val releaseOwner = new CountDownLatch(1)
      try {
        val ownerConf = configuration(fixture, nativeFirst)
        val waiterConf = configuration(fixture, !nativeFirst)
        val ownerStore = store(fixture, ownerConf, nativeFirst)
        val waiterStore = store(fixture, waiterConf, !nativeFirst)
        assert(ownerStore ne waiterStore, "the lock must be shared across store instances")
        val ownerLine = "owner-" + ("x" * size)
        val waiterLine = "waiter-" + ("y" * size)
        val ownerActions = new Iterator[String] {
          private val lines = Iterator(ownerLine)
          override def hasNext: Boolean = {
            // Both implementations build their stream (and legacy checks existence) first.
            ownerReady.countDown()
            assert(releaseOwner.await(60, TimeUnit.SECONDS), "owner was not released")
            lines.hasNext
          }
          override def next(): String = lines.next()
        }
        val owner = pool.submit(new Callable[Try[Unit]] {
          override def call(): Try[Unit] = Try {
            ownerStore.write(fixture.path(key), ownerActions, overwrite = false, ownerConf)
          }
        })
        assert(ownerReady.await(30, TimeUnit.SECONDS), "owner did not build its output stream")
        assert(fixture.bytes(key).isEmpty)
        val waiterThread = new AtomicReference[Thread]
        val waiter = pool.submit(new Callable[Try[Unit]] {
          override def call(): Try[Unit] = Try {
            waiterThread.set(Thread.currentThread())
            waiterStore.write(fixture.path(key), Iterator(waiterLine),
              overwrite = false, waiterConf)
          }
        })
        waitForAttempt(waiter, waiterThread)

        // A shared lock must still allow another key to finish while this owner is blocked.
        val other = pool.submit(new Callable[Try[Unit]] {
          override def call(): Try[Unit] = Try {
            waiterStore.write(fixture.path(otherKey), Iterator("other-key"),
              overwrite = false, waiterConf)
          }
        })
        val otherResult = other.get(30, TimeUnit.SECONDS)
        assert(otherResult.isSuccess, s"another key was blocked or failed: $otherResult")
        assertContents(fixture, otherKey, "other-key", publications = 1)

        releaseOwner.countDown()
        val ownerResult = owner.get(60, TimeUnit.SECONDS)
        val waiterResult = waiter.get(60, TimeUnit.SECONDS)
        assert(ownerResult.isSuccess, s"owner failed: $ownerResult")
        assert(waiterResult.failed.toOption.exists(_.isInstanceOf[FileAlreadyExistsException]),
          s"waiter must report a java.nio.file conflict: $waiterResult")
        assertContents(fixture, key, ownerLine, publications = 1)
        val publications = fixture.requests.filter(r =>
          r.key == key && (r.operation == Put || r.operation == Complete) && r.accepted)
        assert(publications.size == 1)
        assert(publications.head.operation == operation)
        assert(publications.head.conditional == nativeFirst)
        if (operation == Complete) {
          assert(fixture.requests.count(r => r.key == key && r.operation == Part) >= 2)
        }
        assert(fixture.pendingUploads == 0)
        assert(fixture.handlerErrors.isEmpty, fixture.handlerErrors.mkString("\n"))
      } finally {
        releaseOwner.countDown()
        pool.shutdownNow()
        try {
          assert(pool.awaitTermination(30, TimeUnit.SECONDS), "writers did not stop")
        } finally fixture.close()
      }
    }
  }


  test("native recovery holds the path lock until the ownership HEAD completes") {
    val fixture = new S3NativeTestFixture
    val pool = ThreadUtils.newDaemonFixedThreadPool(2, "shared-s3-recovery-lock")
    val gate = new SharedS3LogStoreLockSuite.RecoveryGate
    SharedS3LogStoreLockSuite.recoveryGates.put(fixture.endpoint, gate)
    try {
      val ownerConf = configuration(fixture, native = true)
      ownerConf.set("fs.s3a.impl", classOf[SharedS3RecoveryGatedFileSystem].getName)
      val overwriteConf = configuration(fixture, native = false)
      val ownerStore = store(fixture, ownerConf, native = true)
      val overwriteStore = store(fixture, overwriteConf, native = false)
      fixture.loseNextResponse(Put, key)
      val owner = pool.submit(new Callable[Try[Unit]] {
        override def call(): Try[Unit] = Try {
          ownerStore.write(fixture.path(key), Iterator("recovering-owner"),
            overwrite = false, ownerConf)
        }
      })
      assert(gate.reached.await(30, TimeUnit.SECONDS), "owner did not reach its recovery HEAD")
      assertContents(fixture, key, "recovering-owner", publications = 1)
      assert(fixture.requests.exists(r =>
        r.operation == Put && r.key == key && r.accepted && r.status == 0),
        "the fixture must accept the owner's PUT and drop its acknowledgement")
      assert(fixture.requests.exists(r => r.operation == Head && r.key == key && r.status == 200),
        "recovery must first confirm the published object is a file")

      val overwriteThread = new AtomicReference[Thread]
      val overwrite = pool.submit(new Callable[Try[Unit]] {
        override def call(): Try[Unit] = Try {
          overwriteThread.set(Thread.currentThread())
          overwriteStore.write(fixture.path(key), Iterator("replacement"),
            overwrite = true, overwriteConf)
        }
      })
      waitForAttempt(overwrite, overwriteThread)
      gate.release.countDown()
      val ownerResult = owner.get(60, TimeUnit.SECONDS)
      val overwriteResult = overwrite.get(60, TimeUnit.SECONDS)
      assert(ownerResult.isSuccess, s"overwrite invalidated the owner's recovery: $ownerResult")
      assert(overwriteResult.isSuccess, s"overwrite failed: $overwriteResult")
      assertContents(fixture, key, "replacement", publications = 2)
      val publications = fixture.requests.filter(r => r.key == key && r.accepted)
      assert(publications.map(_.conditional) == Seq(true, false))
      assert(fixture.pendingUploads == 0)
      assert(fixture.handlerErrors.isEmpty, fixture.handlerErrors.mkString("\n"))
    } finally {
      gate.release.countDown()
      pool.shutdownNow()
      try {
        assert(pool.awaitTermination(30, TimeUnit.SECONDS), "writers did not stop")
      } finally {
        SharedS3LogStoreLockSuite.recoveryGates.remove(fixture.endpoint)
        fixture.close()
      }
    }
  }
}

private[delta] object SharedS3LogStoreLockSuite {
  final class RecoveryGate {
    val reached = new CountDownLatch(1)
    val release = new CountDownLatch(1)
  }

  val recoveryGates = new ConcurrentHashMap[String, RecoveryGate]
}

/** Gates only the owner's recovery HEAD; all storage operations still use real S3A/HTTP. */
class SharedS3RecoveryGatedFileSystem extends S3NativeTrackedFileSystem {
  override def getXAttr(path: Path, name: String): Array[Byte] = {
    Option(SharedS3LogStoreLockSuite.recoveryGates.get(getConf.get("fs.s3a.endpoint")))
      .foreach { gate =>
        gate.reached.countDown()
        if (!gate.release.await(60, TimeUnit.SECONDS)) {
          throw new IOException("Timed out before the ownership recovery HEAD")
        }
      }
    super.getXAttr(path, name)
  }
}
