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

import java.io.{File, IOException}
import java.net.URLClassLoader
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{FileAlreadyExistsException, Files, Path => LocalPath}
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._

import io.delta.storage.{LogStore, S3LogStore, S3SingleDriverLogStore}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.hadoop.util.VersionInfo
import org.scalatest.funsuite.AnyFunSuite

private[integration] trait S3NativeTestSupport { self: AnyFunSuite =>
  protected val key = "table/_delta_log/00000000000000000000.json"
  protected val largeLine: String = "x" * (6 * 1024 * 1024)

  protected def withFixture(f: S3NativeTestFixture => Unit): Unit = withSettings(Map.empty)(f)

  protected def withSettings(settings: Map[String, String])(
      f: S3NativeTestFixture => Unit): Unit = {
    val fixture = new S3NativeTestFixture(settings)
    try {
      f(fixture)
      assert(fixture.handlerErrors.isEmpty, fixture.handlerErrors.mkString("\n"))
    } finally fixture.close()
  }

  protected def nativeStore(conf: Configuration): LogStore = new S3LogStore(conf)

  protected def write(store: LogStore, fixture: S3NativeTestFixture,
      line: String, overwrite: Boolean = false): Unit = {
    store.write(fixture.path(key), Seq(line).asJava.iterator(), overwrite, fixture.configuration)
  }

  protected def content(fixture: S3NativeTestFixture): String =
    new String(fixture.bytes(key).getOrElse(fail("No object published")), UTF_8)

  protected def publicationRequests(fixture: S3NativeTestFixture) = {
    import S3NativeTestFixture._
    fixture.requests.filter(r => r.key == key && (r.operation == Put || r.operation == Complete))
  }
}

/** Baseline characterization must pass against the existing single-driver LogStore. */
class S3NativeLegacyCharacterizationSuite extends AnyFunSuite with S3NativeTestSupport {
  test("real Hadoop S3A 3.4.2 legacy create sends an unconditional PUT") {
    withFixture { fixture =>
      assert(VersionInfo.getVersion == "3.4.2")
      val conf = fixture.configuration
      val fs = fixture.path(key).getFileSystem(conf)
      try assert(fs.isInstanceOf[S3AFileSystem]) finally fs.close()
      write(new S3SingleDriverLogStore(conf), fixture, "legacy")
      assert(content(fixture) == "legacy\n")
      val puts = publicationRequests(fixture)
      assert(puts.nonEmpty)
      assert(puts.forall(!_.conditional), "Legacy write unexpectedly used native PIA")
    }
  }

  test("real S3A multipart upload preserves every byte through SDK streaming framing") {
    withFixture { fixture =>
      write(new S3SingleDriverLogStore(fixture.configuration), fixture, largeLine)
      assert(content(fixture) == largeLine + "\n")
      assert(fixture.requests.count(_.operation == S3NativeTestFixture.Part) >= 2)
      assert(publicationRequests(fixture).exists(_.operation == S3NativeTestFixture.Complete))
      assert(fixture.pendingUploads == 0)
    }
  }

  test("real S3A rename preserves bytes and metadata for Spark checksum publication") {
    withFixture { fixture =>
      fixture.seed("table/_delta_log/.checksum.tmp", "checksum".getBytes(UTF_8),
        Map("test-metadata" -> "preserved"))
      val fs = fixture.path(key).getFileSystem(fixture.configuration)
      try {
        assert(fs.rename(fixture.path("table/_delta_log/.checksum.tmp"),
          fixture.path("table/_delta_log/00000000000000000000.crc")))
        assert(fixture.bytes("table/_delta_log/.checksum.tmp").isEmpty)
        assert(new String(fixture.bytes("table/_delta_log/00000000000000000000.crc").get,
          UTF_8) == "checksum")
        assert(fixture.metadata("table/_delta_log/00000000000000000000.crc") ==
          Map("test-metadata" -> "preserved"))
      } finally fs.close()
    }
  }

  test("real S3A checksum rename succeeds when its parent contains multiple siblings") {
    withFixture { fixture =>
      val source = "table/_delta_log/.checksum.tmp"
      val destination = "table/_delta_log/00000000000000000001.crc"
      fixture.seed(source, "checksum".getBytes(UTF_8))
      fixture.seed("table/_delta_log/00000000000000000000.json", "v0".getBytes(UTF_8))
      fixture.seed("table/_delta_log/00000000000000000001.json", "v1".getBytes(UTF_8))
      val fs = fixture.path(key).getFileSystem(fixture.configuration)
      try {
        assert(fs.getFileStatus(fixture.path("table/_delta_log")).isDirectory)
        assert(fs.rename(fixture.path(source), fixture.path(destination)))
        assert(fixture.bytes(source).isEmpty)
        assert(new String(fixture.bytes(destination).get, UTF_8) == "checksum")
        assert(fixture.bytes("table/_delta_log/00000000000000000000.json").isDefined)
        assert(fixture.bytes("table/_delta_log/00000000000000000001.json").isDefined)
      } finally fs.close()
    }
  }

  for (version <- Seq(1, 2)) {
    test(s"real S3A list V$version pages through sibling objects and common prefixes") {
      withSettings(Map("fs.s3a.paging.maximum" -> "1",
          "fs.s3a.list.version" -> version.toString)) { fixture =>
        Seq("a", "b/child", "b/second-child", "c", "d/child").foreach { name =>
          fixture.seed("table/_delta_log/" + name, name.getBytes(UTF_8))
        }
        val fs = fixture.path(key).getFileSystem(fixture.configuration)
        try {
          val names = fs.listStatus(fixture.path("table/_delta_log")).map(_.getPath.getName)
          assert(names.toVector.sorted == Vector("a", "b", "c", "d"))
          val cursor = if (version == 1) "marker" else "continuation-token"
          assert(fixture.requests.exists(r =>
            r.operation == S3NativeTestFixture.List && r.query.contains(cursor)))
          assert(fixture.requests.filter(_.operation == S3NativeTestFixture.List)
            .forall(_.status == 200))
        } finally fs.close()
      }
    }
  }

  test("real S3A GET and list read the legacy object through the HTTP fixture") {
    withFixture { fixture =>
      val conf = fixture.configuration
      val store = new S3SingleDriverLogStore(conf)
      write(store, fixture, "{\"commitInfo\":{}}")
      val read = store.read(fixture.path(key), conf)
      try assert(read.asScala.toVector == Vector("{\"commitInfo\":{}}")) finally read.close()
      assert(store.listFrom(fixture.path(key), conf).asScala.map(_.getPath.getName).toVector ==
        Vector("00000000000000000000.json"))
    }
  }
}

/**
 * No live S3 account is needed. These tests run the real S3A/SDK stack against a loopback endpoint.
 * Lost-response tests cover both disabled SDK retries and retry rejection after acceptance.
 */
class S3NativeIntegrationSuite extends AnyFunSuite with S3NativeTestSupport {
  import S3NativeTestFixture._

  test("native create is conditional and an existing foreign object cannot be replaced") {
    withFixture { fixture =>
      val store = nativeStore(fixture.configuration)
      write(store, fixture, "winner")
      intercept[FileAlreadyExistsException] { write(store, fixture, "loser") }
      assert(content(fixture) == "winner\n")
      assert(fixture.acceptedPublications(key) == 1)
      assert(publicationRequests(fixture).forall(_.conditional))
      assert(publicationRequests(fixture).exists(_.status == 412))
      assert(fixture.metadata(key).nonEmpty, "Publication must carry writer identity metadata")
    }
  }

  for (retries <- Seq(0, 2); operation <- Seq(Put, Complete)) {
    test(s"lost $operation acknowledgement recovers with SDK numRetries=$retries") {
      withSettings(Map("fs.s3a.attempts.maximum" -> retries.toString)) { fixture =>
        val store = nativeStore(fixture.configuration)
        fixture.loseNextResponse(operation, key)
        val line = if (operation == Put) "accepted" else largeLine
        write(store, fixture, line)
        assert(content(fixture) == line + "\n")
        assert(fixture.acceptedPublications(key) == 1)
        val publications = publicationRequests(fixture)
        assert(publications.forall(_.conditional))
        assert(publications.head.accepted && publications.head.status == 0)
        if (retries == 0) {
          assert(publications.size == 1, "SDK must not retry with numRetries=0")
        } else {
          // The accepted MPU's ID no longer exists; the accepted PUT's key now exists.
          val expectedRejection = if (operation == Put) 412 else 404
          assert(publications.map(_.status) == Vector(0, expectedRejection))
        }
        assert(fixture.requests.exists(r => r.key == key && r.operation == Head && r.status == 200))
        assert(fixture.pendingUploads == 0)
      }
    }
  }

  test("foreign writer with identical bytes is still a conflict and preserves original metadata") {
    withFixture { fixture =>
      val store = nativeStore(fixture.configuration)
      write(store, fixture, "identical")
      val winnerMetadata = fixture.metadata(key)
      intercept[FileAlreadyExistsException] { write(store, fixture, "identical") }
      assert(content(fixture) == "identical\n")
      assert(winnerMetadata.nonEmpty && fixture.metadata(key) == winnerMetadata)
      assert(fixture.acceptedPublications(key) == 1)
      assert(publicationRequests(fixture).exists(_.status == 412))
    }
  }

  test("an unavailable ownership probe reports unknown, not a foreign conflict or success") {
    withFixture { fixture =>
      val store = nativeStore(fixture.configuration)
      fixture.loseNextResponse(Put, key)
      fixture.failHeadAfterAcceptance(key)
      val error = intercept[IOException] { write(store, fixture, "accepted-but-unknown") }
      assert(!error.isInstanceOf[FileAlreadyExistsException])
      assert(content(fixture) == "accepted-but-unknown\n")
      assert(fixture.acceptedPublications(key) == 1)
      assert(fixture.requests.exists(r => r.key == key && r.operation == Head && r.status == 503))
    }
  }

  test("a key slash marker does not make the absent exact key a foreign conflict") {
    withFixture { fixture =>
      val store = nativeStore(fixture.configuration)
      fixture.seed(key + "/", Array.emptyByteArray)
      write(store, fixture, "exact-key")
      assert(content(fixture) == "exact-key\n")
      assert(fixture.bytes(key + "/").exists(_.isEmpty))
      assert(publicationRequests(fixture).forall(_.conditional))
    }
  }

  test("overwrite true still replaces an object with an unconditional write") {
    withFixture { fixture =>
      val store = nativeStore(fixture.configuration)
      fixture.seed(key, "old\n".getBytes(UTF_8))
      write(store, fixture, "new", overwrite = true)
      assert(content(fixture) == "new\n")
      assert(publicationRequests(fixture).exists(!_.conditional))
    }
  }

  test("multipart completion alone publishes the full object conditionally") {
    withFixture { fixture =>
      val store = nativeStore(fixture.configuration)
      write(store, fixture, largeLine)
      assert(content(fixture) == largeLine + "\n")
      assert(fixture.requests.count(r => r.operation == Part && r.key == key) >= 2)
      assert(publicationRequests(fixture).exists(r => r.operation == Complete && r.conditional))
      assert(!publicationRequests(fixture).exists(_.operation == Put))
      assert(fixture.pendingUploads == 0)
    }
  }

  test("HTTP 200 embedded Complete error cannot be mistaken for successful publication") {
    withSettings(Map("fs.s3a.attempts.maximum" -> "0")) { fixture =>
      val store = nativeStore(fixture.configuration)
      fixture.failNext(Complete, key, 200, "PreconditionFailed", times = 20)
      val error = intercept[IOException] { write(store, fixture, largeLine) }
      assert(!error.isInstanceOf[FileAlreadyExistsException])
      assert(fixture.bytes(key).isEmpty)
      assert(fixture.acceptedPublications(key) == 0)
      val completions = publicationRequests(fixture)
      assert(completions.nonEmpty && completions.forall(r => r.status == 200 && !r.accepted))
      assert(fixture.requests.exists(r => r.key == key && r.operation == Head && r.status == 404))
      assert(fixture.pendingUploads == 0)
    }
  }

  for ((operation, status, code, line) <- Seq(
      (Put, 409, "ConditionalRequestConflict", "small"),
      (Complete, 409, "ConditionalRequestConflict", largeLine),
      (Complete, 404, "NoSuchUpload", largeLine))) {
    test(s"$operation $code with no exact object remains an unresolved IOException") {
      withFixture { fixture =>
        val store = nativeStore(fixture.configuration)
        // Persistent injection prevents a hidden SDK retry from silently turning this into success.
        fixture.failNext(operation, key, status, code, times = 20)
        fixture.seed(key + "/", Array.emptyByteArray)
        val error = intercept[IOException] { write(store, fixture, line) }
        assert(!error.isInstanceOf[FileAlreadyExistsException])
        assert(fixture.bytes(key).isEmpty)
        assert(fixture.acceptedPublications(key) == 0)
        assert(publicationRequests(fixture).exists(_.status == status))
        assert(publicationRequests(fixture).size <= 5, "Publication retries must be bounded")
        assert(fixture.pendingUploads == 0, "Failed multipart uploads must be aborted")
      }
    }
  }

  for ((operation, size) <- Seq(Put -> 0, Complete -> (6 * 1024 * 1024))) {
    test(s"independent JVM $operation writers have exactly one winner for the same absent key") {
      withFixture { fixture =>
        val gate = fixture.pause(operation, key, arrivals = 2)
        try {
          withWorker(fixture, "first", size) { first =>
            withWorker(fixture, "second", size) { second =>
              assert(gate.await(),
                s"Both writers did not reach $operation\n${first.output}\n${second.output}")
              assert(fixture.bytes(key).isEmpty)
              gate.close()
              val results = Vector(first.result(), second.result())
              assert(results.sorted == Vector(0, 3), results.mkString(","))
              val expected = Set("first", "second").map(_ + ("x" * size) + "\n")
              assert(expected.contains(content(fixture)))
              assert(fixture.acceptedPublications(key) == 1)
              assert(publicationRequests(fixture).count(_.status == 412) >= 1)
              assert(publicationRequests(fixture).forall(_.conditional))
              assert(fixture.pendingUploads == 0)
            }
          }
        } finally gate.close()
      }
    }
  }

  test("killing a JVM before multipart publication leaves no partial object") {
    withFixture { fixture =>
      val store = nativeStore(fixture.configuration)
      val gate = fixture.pause(Part, key)
      val worker = startWorker(fixture, "killed", 6 * 1024 * 1024)
      try {
        assert(gate.await(), worker.output)
        assert(fixture.requests.exists(r => r.key == key && r.operation == Part))
        assert(fixture.bytes(key).isEmpty)
        worker.kill()
        assert(!fixture.requests.exists(r => r.key == key && r.operation == Complete))
        gate.close()
        write(store, fixture, "replacement")
        assert(content(fixture) == "replacement\n")
        assert(fixture.acceptedPublications(key) == 1)
        // A killed process cannot clean up its MPU. S3 lifecycle expiration is outside this test.
      } finally { gate.close(); worker.close() }
    }
  }

  for ((operation, size) <- Seq(Put -> 0, Complete -> (6 * 1024 * 1024))) {
    test(s"killing a JVM after $operation acceptance preserves the winner against fresh writers") {
      withFixture { fixture =>
        val store = nativeStore(fixture.configuration)
        val gate = fixture.pause(operation, key, afterAcceptance = true)
        try {
          withWorker(fixture, "accepted-before-kill", size) { worker =>
            assert(gate.await(), worker.output)
            val expected = "accepted-before-kill" + ("x" * size) + "\n"
            assert(content(fixture) == expected)
            worker.kill()
            gate.close()
            intercept[FileAlreadyExistsException] { write(store, fixture, "replacement") }
            assert(content(fixture) == expected)
            assert(fixture.acceptedPublications(key) == 1)
            assert(fixture.pendingUploads == 0)
          }
        } finally gate.close()
      }
    }
  }

  private def withWorker(fixture: S3NativeTestFixture, label: String, size: Int)(
      f: Worker => Unit): Unit = {
    val worker = startWorker(fixture, label, size)
    try f(worker) finally worker.close()
  }

  private class Worker(val process: Process, log: LocalPath) extends AutoCloseable {
    def output: String = new String(Files.readAllBytes(log), UTF_8)
    def result(): Int = {
      assert(process.waitFor(60, TimeUnit.SECONDS), s"Worker timed out: $output")
      val code = process.exitValue()
      assert(code == 0 || code == 3, s"Unexpected worker exit $code: $output")
      code
    }
    def kill(): Unit = {
      process.destroyForcibly()
      assert(process.waitFor(10, TimeUnit.SECONDS), "Worker did not terminate")
    }
    override def close(): Unit = {
      try { if (process.isAlive) kill() } finally Files.deleteIfExists(log)
    }
  }

  private def startWorker(fixture: S3NativeTestFixture, label: String, size: Int): Worker = {
    // sbt may use layered classloaders, so java.class.path alone is insufficient.
    def loaderEntries(loader: ClassLoader): Vector[String] = {
      if (loader == null) Vector.empty else {
        val entries = loader match {
          case urls: URLClassLoader => urls.getURLs.toVector.filter(_.getProtocol == "file")
            .map(url => new File(url.toURI).getAbsolutePath)
          case _ => Vector.empty
        }
        entries ++ loaderEntries(loader.getParent)
      }
    }
    val classpath = (System.getProperty("java.class.path").split(File.pathSeparator).toVector ++
      loaderEntries(getClass.getClassLoader) ++
      loaderEntries(Thread.currentThread().getContextClassLoader)).distinct.mkString(File.pathSeparator)
    val log = Files.createTempFile("s3-native-worker-", ".log")
    val java = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
    try {
      val process = new ProcessBuilder(java, "-Xmx256m", "-cp", classpath,
        "io.delta.storage.integration.S3NativeJvmWorker", fixture.endpoint, fixture.bucket,
        key, label, size.toString).redirectErrorStream(true).redirectOutput(log.toFile).start()
      new Worker(process, log)
    } catch {
      case failure: IOException => Files.deleteIfExists(log); throw failure
    }
  }
}
