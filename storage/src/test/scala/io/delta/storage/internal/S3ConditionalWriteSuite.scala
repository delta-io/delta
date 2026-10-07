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

package io.delta.storage.internal

import java.io.{ByteArrayOutputStream, FileNotFoundException, IOException, InterruptedIOException, OutputStream}
import java.net.SocketTimeoutException
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.FileAlreadyExistsException

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Abortable, FileStatus, FileSystem, FSDataOutputStream, FSDataOutputStreamBuilder, Path, StreamCapabilities}
import org.apache.hadoop.fs.s3a.{AWSServiceIOException, RemoteFileChangedException, S3AFileSystem}
import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.services.s3.model.S3Exception

/** Faults at the connector boundary which are awkward to induce with an HTTP server. */
class S3ConditionalWriteSuite extends AnyFunSuite {
  private val path = new Path("s3a://bucket/_delta_log/00000000000000000001.json")
  private val conditional = "fs.option.create.conditional.overwrite"
  private val header = "fs.s3a.create.header.delta-log-store-write-id"

  private def write(fs: FileSystem, actions: Iterator[String] = Iterator("first", "λ")): Unit = {
    S3ConditionalWrite.write(fs, path, actions.asJava)
  }

  private def conflict = new RemoteFileChangedException(path.toString, "close", "412")
  private def serviceFailure(status: Int): IOException = new AWSServiceIOException(
    "publish", S3Exception.builder().statusCode(status).message("injected").build())

  test("publication requires the absent condition and unique owner, preserving action bytes") {
    val fs = new ScriptedS3A
    write(fs)
    assert(fs.stream.bytes.toSeq == "first\nλ\n".getBytes(UTF_8).toSeq)
    assert(fs.stream.closed)
    assert(fs.mandatory.contains(conditional) && fs.mandatory.contains(header))
    assert(fs.options.getBoolean(conditional, false))
    assert(!fs.overwrite)
    assert(fs.owner.nonEmpty)
    assert(fs.probes == 0)
    val next = new ScriptedS3A
    write(next, Iterator.empty)
    assert(next.owner != fs.owner)
    assert(next.stream.bytes.isEmpty && next.stream.closed)
  }

  Seq[(String, () => IOException)](
    "translated 412" -> (() => conflict),
    "raw 412" -> (() => serviceFailure(412)),
    "409" -> (() => serviceFailure(409)),
    "consumed multipart upload" -> (() => serviceFailure(404)),
    "transport timeout" -> (() => new SocketTimeoutException("lost response")),
    "access denied after completion" -> (() => serviceFailure(403))
  ).foreach { case (name, failure) =>
    test(s"recover own completed payload after $name") {
      val fs = new ScriptedS3A
      fs.stream.closeFailure = failure()
      write(fs)
      assert(fs.probes == 1)
      assert(!Thread.currentThread().isInterrupted)
    }
  }

  Seq[(String, () => IOException)](
    "translated 412" -> (() => conflict),
    "raw 412" -> (() => serviceFailure(412)),
    "409" -> (() => serviceFailure(409)),
    "Hadoop existence exception" ->
      (() => new org.apache.hadoop.fs.FileAlreadyExistsException("exists"))
  ).foreach { case (name, failure) =>
    for (owner <- Seq(Some("another-writer"), None)) {
      test(s"$name with foreign or missing owner $owner is a conflict") {
        val fs = new ScriptedS3A
        val original = failure()
        fs.stream.closeFailure = original
        fs.readOwner = () => owner.map(_.getBytes(UTF_8)).orNull
        val error = intercept[FileAlreadyExistsException](write(fs))
        assert(error.getCause eq original)
      }
    }
  }

  test("a foreign object does not turn an arbitrary close failure into a conflict") {
    val fs = new ScriptedS3A
    val original = new IOException("lost connection")
    fs.stream.closeFailure = original
    fs.readOwner = () => "another-writer".getBytes(UTF_8)
    val error = intercept[IOException](write(fs))
    assert(!error.isInstanceOf[FileAlreadyExistsException])
    assert(error.getCause eq original)
    assert(error.getMessage.toLowerCase.contains("unknown"))
  }

  for (status <- Seq(409, 412, 404)) {
    test(s"$status without a destination is unknown and never replays the iterator") {
      val fs = new ScriptedS3A
      val original = serviceFailure(status)
      fs.stream.closeFailure = original
      fs.statusFailure = new FileNotFoundException("absent")
      val error = intercept[IOException](write(fs))
      assert(!error.isInstanceOf[FileAlreadyExistsException])
      assert(error.getCause eq original)
      assert(fs.builds == 1)
      assert(error.getSuppressed.contains(fs.statusFailure))
    }
  }

  test("directory marker must not masquerade as an existing commit") {
    val fs = new ScriptedS3A
    fs.stream.closeFailure = serviceFailure(409)
    fs.directory = true
    val error = intercept[IOException](write(fs))
    assert(!error.isInstanceOf[FileAlreadyExistsException])
    assert(fs.probes == 0)
  }

  test("failed ownership probe retains the publication and probe failures") {
    val fs = new ScriptedS3A
    val original = conflict
    val headFailure = new IOException("HEAD denied")
    fs.stream.closeFailure = original
    fs.readOwner = () => throw headFailure
    val error = intercept[IOException](write(fs))
    assert(error.getCause eq original)
    assert(error.getSuppressed.contains(headFailure))
  }

  test("build-time Hadoop existence failure follows the Java LogStore contract") {
    val fs = new ScriptedS3A
    val original = new org.apache.hadoop.fs.FileAlreadyExistsException("exists")
    fs.buildFailure = original
    val error = intercept[FileAlreadyExistsException](write(fs))
    assert(error.getCause eq original)
    assert(!fs.stream.closed && fs.probes == 0)
  }

  test("iterator failure aborts without publishing its prefix") {
    val fs = new ScriptedS3A
    val original = new IllegalStateException("iterator failed")
    val actions = Iterator("prefix") ++ Iterator.continually(throw original)
    val error = intercept[IllegalStateException](write(fs, actions))
    assert(error eq original)
    assert(fs.stream.aborted && !fs.stream.closed && fs.probes == 0)
  }

  test("write failure aborts and retains abort cleanup failure") {
    val fs = new ScriptedS3A
    val original = new IOException("write failed")
    val cleanup = new IOException("abort failed")
    fs.stream.writeFailure = original
    fs.stream.abortFailure = cleanup
    val error = intercept[IOException](write(fs))
    assert(error eq original)
    assert(error.getSuppressed.contains(cleanup))
    assert(fs.stream.aborted && !fs.stream.closed && fs.probes == 0)
  }

  for (cap <- Seq(conditional, StreamCapabilities.ABORTABLE_STREAM)) {
    test(s"missing active stream capability $cap fails before consuming actions") {
      val fs = new ScriptedS3A
      fs.stream.capabilities -= cap
      intercept[UnsupportedOperationException](write(fs))
      assert(fs.stream.bytes.isEmpty && !fs.stream.closed && fs.stream.aborted)
    }
  }

  test("magic streams cannot report success before object publication") {
    val fs = new ScriptedS3A
    fs.stream.capabilities += "fs.s3a.capability.magic.output.stream"
    intercept[UnsupportedOperationException](write(fs))
    assert(fs.stream.bytes.isEmpty && !fs.stream.closed && fs.stream.aborted)
  }

  test("unsupported path capabilities fail before opening a stream") {
    val fs = new ScriptedS3A
    fs.pathCapabilities = false
    intercept[UnsupportedOperationException](write(fs))
    assert(fs.builds == 0)
  }

  test("interrupted publication keeps the interrupt and does not probe") {
    val fs = new ScriptedS3A
    fs.stream.closeFailure = new InterruptedIOException("cancelled")
    try {
      val error = intercept[InterruptedIOException](write(fs))
      assert(error.getCause eq fs.stream.closeFailure)
      assert(Thread.currentThread().isInterrupted)
      assert(fs.probes == 0)
    } finally {
      Thread.interrupted()
    }
  }

  test("interrupted stream write restores cancellation after abort") {
    val fs = new ScriptedS3A
    val original = new InterruptedIOException("write cancelled")
    fs.stream.writeFailure = original
    try {
      assert(intercept[InterruptedIOException](write(fs)) eq original)
      assert(Thread.currentThread().isInterrupted)
      assert(fs.stream.aborted && !fs.stream.closed && fs.probes == 0)
    } finally {
      Thread.interrupted()
    }
  }

  test("a stream write timeout aborts without interrupting the thread") {
    val fs = new ScriptedS3A
    val original = new SocketTimeoutException("write timeout")
    fs.stream.writeFailure = original
    assert(intercept[SocketTimeoutException](write(fs)) eq original)
    assert(!Thread.currentThread().isInterrupted)
    assert(fs.stream.aborted && !fs.stream.closed && fs.probes == 0)
  }

  test("interrupted ownership probe preserves both failures and cancellation") {
    val fs = new ScriptedS3A
    val original = conflict
    val cancelled = new InterruptedIOException("HEAD cancelled")
    fs.stream.closeFailure = original
    fs.readOwner = () => throw cancelled
    try {
      val error = intercept[InterruptedIOException](write(fs))
      assert(error.getCause eq original)
      assert(error.getSuppressed.contains(cancelled))
      assert(Thread.currentThread().isInterrupted)
    } finally {
      Thread.interrupted()
    }
  }

  test("interrupted abort preserves the primary failure and cancellation") {
    val fs = new ScriptedS3A
    val original = new IOException("write failed")
    val cancelled = new InterruptedIOException("abort cancelled")
    fs.stream.writeFailure = original
    fs.stream.abortFailure = cancelled
    try {
      val error = intercept[IOException](write(fs))
      assert(error eq original)
      assert(error.getSuppressed.contains(cancelled))
      assert(Thread.currentThread().isInterrupted)
      assert(!fs.stream.closed)
    } finally {
      Thread.interrupted()
    }
  }

  test("unchecked close failure is never success and attempts cleanup") {
    val fs = new ScriptedS3A
    val original = new IllegalStateException("close failed")
    fs.stream.closeFailure = original
    assert(intercept[IllegalStateException](write(fs)) eq original)
    assert(fs.stream.aborted && fs.probes == 0)
  }

  private class ScriptedS3A extends S3AFileSystem {
    setConf(new Configuration(false))
    val stream = new ScriptedStream
    var owner = ""
    var mandatory = Set.empty[String]
    var options = new Configuration(false)
    var overwrite = true
    var builds = 0
    var probes = 0
    var directory = false
    var pathCapabilities = true
    var statusFailure: IOException = null
    var buildFailure: IOException = null
    var readOwner: () => Array[Byte] = () => owner.getBytes(UTF_8)

    override def hasPathCapability(p: Path, capability: String): Boolean = pathCapabilities

    override def getFileStatus(p: Path): FileStatus = {
      if (statusFailure != null) throw statusFailure
      new FileStatus(stream.bytes.length, directory, 1, 1, 0, p)
    }

    override def getXAttr(p: Path, name: String): Array[Byte] = {
      assert(name == "header.delta-log-store-write-id")
      probes += 1
      readOwner()
    }

    override def createFile(p: Path): FSDataOutputStreamBuilder[_, _] =
      new Builder(p)

    // Separate concrete type preserves the self type of Hadoop's builder.
    private class Builder(p: Path)
      extends FSDataOutputStreamBuilder[FSDataOutputStream, Builder](ScriptedS3A.this, p) {
      override def getThisBuilder: Builder = this
      override def build(): FSDataOutputStream = {
        builds += 1
        if (buildFailure != null) throw buildFailure
        mandatory = getMandatoryKeys.asScala.toSet
        options = getOptions
        ScriptedS3A.this.overwrite = getFlags.contains(org.apache.hadoop.fs.CreateFlag.OVERWRITE)
        owner = options.get(header, "")
        new FSDataOutputStream(stream, null)
      }
    }
  }

  private class ScriptedStream extends OutputStream with Abortable with StreamCapabilities {
    private val buffer = new ByteArrayOutputStream
    var closed = false
    var aborted = false
    var capabilities = Set(conditional, StreamCapabilities.ABORTABLE_STREAM)
    var writeFailure: IOException = null
    var closeFailure: Throwable = null
    var abortFailure: IOException = null
    def bytes: Array[Byte] = buffer.toByteArray
    override def write(b: Int): Unit = {
      if (writeFailure != null) throw writeFailure
      buffer.write(b)
    }
    override def close(): Unit = {
      closed = true
      if (closeFailure != null) throw closeFailure
    }
    override def hasCapability(cap: String): Boolean = capabilities.contains(cap)
    override def abort(): Abortable.AbortableResult = {
      aborted = true
      new Abortable.AbortableResult {
        override def alreadyClosed(): Boolean = closed
        override def anyCleanupException(): IOException = abortFailure
      }
    }
  }
}
