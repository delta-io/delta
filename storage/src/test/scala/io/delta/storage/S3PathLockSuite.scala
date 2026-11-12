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

package io.delta.storage

import java.io.InterruptedIOException
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.concurrent.{Callable, CountDownLatch, FutureTask, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, RawLocalFileSystem}
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}

class S3PathLockSuite extends AnyFunSuite with Eventually {
  test("an interrupted waiter preserves cancellation and does not release the owner's path lock") {
    val directory = Files.createTempDirectory("s3-path-lock-")
    val destination = directory.resolve("log.json")
    val path = new Path(destination.toUri)
    val conf = new Configuration(false)
    conf.set("fs.file.impl", classOf[RawLocalFileSystem].getName)
    val ownerReady = new CountDownLatch(1)
    val releaseOwner = new CountDownLatch(1)
    val waiterConsumed = new AtomicBoolean(false)
    val ownerActions = Iterator.single("owner").map { action =>
      ownerReady.countDown()
      assert(releaseOwner.await(30, TimeUnit.SECONDS), "owner was not released")
      action
    }
    val ownerTask = new FutureTask[Try[Unit]](new Callable[Try[Unit]] {
      override def call(): Try[Unit] = Try {
        new S3SingleDriverLogStore(conf).write(path, ownerActions.asJava, false, conf)
      }
    })
    val waiterTask = new FutureTask[(Try[Unit], Boolean)](new Callable[(Try[Unit], Boolean)] {
      override def call(): (Try[Unit], Boolean) = {
        val actions = Iterator.single("waiter").map { action =>
          waiterConsumed.set(true)
          action
        }
        val result = Try {
          new S3SingleDriverLogStore(conf).write(path, actions.asJava, false, conf)
        }
        (result, Thread.currentThread().isInterrupted)
      }
    })
    val owner = new Thread(ownerTask, "s3-lock-owner")
    val waiter = new Thread(waiterTask, "s3-lock-waiter")
    try {
      owner.start()
      assert(ownerReady.await(30, TimeUnit.SECONDS), "owner did not acquire the path lock")
      waiter.start()
      eventually(timeout(Span(30, Seconds)), interval(Span(10, Millis))) {
        assert(waiter.getState == Thread.State.WAITING && waiter.getStackTrace.exists(frame =>
          frame.getClassName == "io.delta.storage.internal.PathLock" &&
            frame.getMethodName == "acquire"))
      }
      waiter.interrupt()
      val (waiterResult, interrupted) = waiterTask.get(30, TimeUnit.SECONDS)
      releaseOwner.countDown()
      val ownerResult = ownerTask.get(30, TimeUnit.SECONDS)

      assert(ownerResult.isSuccess, s"the waiter's cancellation damaged the owner: $ownerResult")
      assert(waiterResult.failed.get.isInstanceOf[InterruptedIOException])
      assert(waiterResult.failed.get.getCause.isInstanceOf[InterruptedException])
      assert(interrupted, "the interrupted waiter must retain its interrupt flag")
      assert(!waiterConsumed.get())
      assert(new String(Files.readAllBytes(destination), UTF_8) == "owner\n")
    } finally {
      releaseOwner.countDown()
      if (waiter.isAlive) waiter.interrupt()
      owner.join(30000)
      waiter.join(30000)
      Files.deleteIfExists(destination)
      Files.deleteIfExists(directory)
    }
  }
}
