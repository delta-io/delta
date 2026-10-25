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

package io.delta.storage

import java.io.{File, InterruptedIOException}
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

/**
 * Verifies that a writer interrupted while waiting for the per-path lock in
 * [[S3SingleDriverLogStore]] does not release a lock held by another writer.
 */
class S3SingleDriverLogStoreLockSuite extends AnyFunSuite {

  private val hadoopConf = new Configuration()

  /** Runs `body` on a new thread and records whatever it throws. */
  private def startWriter(name: String)(body: => Unit): (Thread, AtomicReference[Throwable]) = {
    val error = new AtomicReference[Throwable]()
    val t = new Thread(() => {
      try body catch { case e: Throwable => error.set(e) }
    }, name)
    t.setDaemon(true)
    t.start()
    (t, error)
  }

  private def awaitState(t: Thread, state: Thread.State): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
    while (t.getState != state) {
      assert(System.nanoTime() < deadline, s"${t.getName} never reached $state")
      Thread.sleep(5)
    }
  }

  test("interrupted waiter does not release the lock held by another writer") {
    val tempDir = Files.createTempDirectory("s3-single-driver-lock").toFile
    try {
      val store = new S3SingleDriverLogStore(hadoopConf)
      val path = new Path(new File(tempDir, "_delta_log/00000000000000000000.json").toURI)

      // Writer 1 acquires the lock and then blocks while its actions are being written.
      val insideCriticalSection = new CountDownLatch(1)
      val finishWriting = new CountDownLatch(1)
      val blockingActions = new java.util.Iterator[String] {
        private var emitted = false
        override def hasNext: Boolean = !emitted
        override def next(): String = {
          emitted = true
          insideCriticalSection.countDown()
          finishWriting.await()
          "writer-1"
        }
      }
      val (writer1, writer1Error) = startWriter("writer-1") {
        store.write(path, blockingActions, false, hadoopConf)
      }
      assert(insideCriticalSection.await(30, TimeUnit.SECONDS))

      // Writer 2 waits for the same lock and is interrupted while waiting.
      val (writer2, writer2Error) = startWriter("writer-2") {
        store.write(path, Iterator("writer-2").asJava, false, hadoopConf)
      }
      awaitState(writer2, Thread.State.WAITING)
      writer2.interrupt()
      writer2.join(TimeUnit.SECONDS.toMillis(30))
      assert(writer2Error.get().isInstanceOf[InterruptedIOException])

      // Writer 1 still holds the lock, so writer 3 must block until writer 1 is done. On S3 the
      // object only becomes visible when writer 1 closes its stream, so a writer 3 that got past
      // the lock here would pass the exists() check and overwrite writer 1's commit.
      val (writer3, writer3Error) = startWriter("writer-3") {
        store.write(path, Iterator("writer-3").asJava, false, hadoopConf)
      }
      writer3.join(TimeUnit.SECONDS.toMillis(2))
      assert(writer3.isAlive, "writer-3 entered the critical section while writer-1 held the " +
        s"lock: ${writer3Error.get()}")

      // Once writer 1 finishes, its write must succeed and writer 3 must see the file.
      finishWriting.countDown()
      writer1.join(TimeUnit.SECONDS.toMillis(30))
      writer3.join(TimeUnit.SECONDS.toMillis(30))
      assert(writer1Error.get() === null, s"writer-1 failed: ${writer1Error.get()}")
      assert(writer3Error.get().isInstanceOf[java.nio.file.FileAlreadyExistsException])
      assert(store.read(path, hadoopConf).asScala.toList === List("writer-1"))
    } finally {
      deleteRecursively(tempDir)
    }
  }

  private def deleteRecursively(dir: File): Unit = {
    Option(dir.listFiles()).foreach(_.foreach(deleteRecursively))
    dir.delete()
  }
}
