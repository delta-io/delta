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

package io.delta.storage.integration

import java.net.{InetAddress, ServerSocket, Socket, URI}
import java.nio.charset.StandardCharsets
import java.nio.file.{FileAlreadyExistsException, Paths}
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import io.delta.storage.S3LogStore
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuite

/**
 * S3 integration tests for the conditional-write LogStore.
 *
 * <p>Run these tests with {@code python run-integration-tests.py --s3-log-store-only} after
 * setting {@code S3_LOG_STORE_TEST_BUCKET} and {@code S3_LOG_STORE_TEST_RUN_UID}. A custom
 * endpoint also requires {@code S3_LOG_STORE_TEST_ENDPOINT}, {@code
 * S3_LOG_STORE_TEST_ACCESS_KEY}, and {@code S3_LOG_STORE_TEST_SECRET_KEY}.</p>
 */
class S3LogStoreIntegrationTest extends AnyFunSuite {
  private val runIntegrationTests: Boolean =
    Option(System.getenv("S3_LOG_STORE_TEST_ENABLED")).exists(_.toBoolean)
  private val bucket = System.getenv("S3_LOG_STORE_TEST_BUCKET")
  private val testRunUID = System.getenv("S3_LOG_STORE_TEST_RUN_UID")
  private val configuration = S3IntegrationTestUtils.configuration()
  private lazy val fs: S3AFileSystem = {
    val fileSystem = new S3AFileSystem()
    fileSystem.initialize(new URI(s"s3a://$bucket"), configuration)
    fileSystem
  }
  private lazy val logStore = new S3LogStore(configuration)
  private val integrationTestTag = Tag("IntegrationTest")

  private def integrationTest(name: String)(testFun: => Any): Unit =
    if (runIntegrationTests) test(name, integrationTestTag)(testFun)

  private def path(testName: String): Path =
    new Path(s"s3a://$bucket/$testRunUID/$testName/_delta_log/00000000000000000001.json")

  private def read(path: Path): Seq[String] = {
    val lines = logStore.read(path, configuration)
    try {
      lines.asScala.toSeq
    } finally {
      lines.close()
    }
  }

  private def assertWriteId(path: Path): Unit = {
    val writeId = fs.getXAttr(path, "header.delta-log-store-write-id")
    assert(writeId !== null)
    assert(new String(writeId, StandardCharsets.UTF_8).nonEmpty)
  }

  integrationTest("conditional-create writes content and rejects a duplicate") {
    val commit = path("s3-log-store-create")
    logStore.write(commit, Iterator("one", "♥").asJava, false, configuration)

    assert(read(commit) === Seq("one", "♥"))
    assertWriteId(commit)
    assertThrows[FileAlreadyExistsException] {
      logStore.write(commit, Iterator("duplicate").asJava, false, configuration)
    }
  }

  integrationTest("multipart conditional-create writes content and rejects a duplicate") {
    val commit = path("s3-log-store-multipart-create")
    val action = "x" * (6 * 1024 * 1024)

    logStore.write(commit, Iterator(action).asJava, false, configuration)

    assert(read(commit) === Seq(action))
    assertWriteId(commit)
    assertThrows[FileAlreadyExistsException] {
      logStore.write(commit, Iterator(action).asJava, false, configuration)
    }
  }

  integrationTest("concurrent LogStore instances create exactly one commit object") {
    val commit = path("s3-log-store-concurrent")
    val writerCount = 8
    val start = new CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(writerCount)

    try {
      val attempts = (0 until writerCount).map { writerId =>
        executor.submit(new Callable[Option[String]] {
          override def call(): Option[String] = {
            start.await()
            val action = s"writer-$writerId"
            try {
              new S3LogStore(configuration)
                .write(commit, Iterator(action).asJava, false, configuration)
              Some(action)
            } catch {
              case _: FileAlreadyExistsException => None
            }
          }
        })
      }

      start.countDown()
      val winners = attempts.flatMap(_.get(2, TimeUnit.MINUTES))
      assert(winners.size === 1)
      assert(read(commit) === winners)
    } finally {
      executor.shutdownNow()
    }
  }

  integrationTest("separate JVM writers create exactly one commit object") {
    val commit = path("s3-log-store-separate-jvm-concurrent")
    val writerCount = 4
    val javaExecutable = Paths.get(System.getProperty("java.home"), "bin", "java").toString
    val classPath = System.getProperty("java.class.path")
    val writerClass = S3LogStoreIntegrationWriter.getClass.getName.stripSuffix("$")
    val barrier = new ServerSocket(0, writerCount, InetAddress.getLoopbackAddress)
    barrier.setSoTimeout(TimeUnit.MINUTES.toMillis(2).toInt)
    val attempts = ArrayBuffer.empty[(String, Process)]
    val barrierClients = ArrayBuffer.empty[Socket]

    try {
      (0 until writerCount).foreach { writerId =>
        val action = s"writer-$writerId"
        val process = new ProcessBuilder(
          javaExecutable,
          "-cp",
          classPath,
          writerClass,
          commit.toString,
          action,
          barrier.getLocalPort.toString)
          .inheritIO()
          .start()
        attempts += action -> process
      }

      (0 until writerCount).foreach { _ => barrierClients += barrier.accept() }
      barrierClients.foreach { client =>
        client.getOutputStream.write(1)
        client.getOutputStream.flush()
      }
      barrierClients.foreach(closeQuietly)
      barrierClients.clear()

      val results = attempts.map { case (action, process) =>
        if (!process.waitFor(2, TimeUnit.MINUTES)) {
          fail(s"Separate JVM writer for $action did not finish within two minutes.")
        }
        action -> process.exitValue()
      }.toSeq
      assert(results.map(_._2).sorted ===
        Seq(0) ++ Seq.fill(writerCount - 1)(S3LogStoreIntegrationWriter.ConflictExitCode))
      val winner = results.collect { case (action, 0) => action }
      assert(read(commit) === winner)
    } finally {
      barrierClients.foreach(closeQuietly)
      closeQuietly(barrier)
      attempts.foreach { case (_, process) => destroyProcess(process) }
    }
  }

  private def closeQuietly(closeable: AutoCloseable): Unit = {
    try closeable.close()
    catch {
      case _: Exception =>
    }
  }

  private def destroyProcess(process: Process): Unit = {
    if (process.isAlive) {
      process.destroy()
      if (!process.waitFor(5, TimeUnit.SECONDS)) {
        process.destroyForcibly()
        process.waitFor(5, TimeUnit.SECONDS)
      }
    }
  }
}
