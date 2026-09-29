/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

import java.io.{ByteArrayOutputStream, FileNotFoundException, IOException, OutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{FileAlreadyExistsException => NioFileAlreadyExistsException}
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Options.CreateFileOptionKeys.FS_OPTION_CREATE_CONDITIONAL_OVERWRITE
import org.apache.hadoop.fs.s3a.{AWSServiceIOException, Constants, RemoteFileChangedException}
import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.services.s3.model.S3Exception

class S3ConditionalWriteSuite extends AnyFunSuite {

  private val path = new Path("s3a://bucket/table/_delta_log/00000000000000000001.json")
  private val writeIdHeader =
    s"${Constants.FS_S3A_CREATE_HEADER}.delta-log-store-write-id"
  private val writeIdXAttr =
    s"${Constants.XA_HEADER_PREFIX}delta-log-store-write-id"

  test("conditional write requires create-if-absent and owner metadata") {
    val fs = new TestFileSystem
    val stream = fs.enqueueStream()

    S3ConditionalWrite.write(fs, path, Iterator("first", "second").asJava)

    assert(fs.buildCount == 1)
    assert(fs.mandatoryKeys.head ==
      Set(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE, writeIdHeader))
    assert(fs.options.head.getBoolean(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE, false))
    UUID.fromString(fs.writeIds.head)
    assert(fs.options.head.get(writeIdHeader) == fs.writeIds.head)
    assert(new String(stream.bytes, UTF_8) == "first\nsecond\n")
    assert(fs.xAttrReads == 0)
  }

  test("lost response is success when destination has this write owner") {
    val fs = new TestFileSystem
    val lostResponse = remoteFileChanged()
    val stream = fs.enqueueStream(closeFailure = Some(lostResponse))
    fs.readXAttr = (_, name) => {
      assert(name == writeIdXAttr)
      fs.writeIds.head.getBytes(UTF_8)
    }

    S3ConditionalWrite.write(fs, path, Iterator("payload").asJava)

    assert(new String(stream.bytes, UTF_8) == "payload\n")
    assert(fs.xAttrReads == 1)
  }

  test("same-content foreign owner remains a conflict") {
    val fs = new TestFileSystem
    val stream = fs.enqueueStream(closeFailure = Some(remoteFileChanged()))
    fs.readXAttr = (_, _) => "foreign-write".getBytes(UTF_8)

    val error = intercept[NioFileAlreadyExistsException] {
      S3ConditionalWrite.write(fs, path, Iterator("identical-payload").asJava)
    }

    assert(error.getFile == path.toString)
    assert(error.getCause.isInstanceOf[RemoteFileChangedException])
    assert(new String(stream.bytes, UTF_8) == "identical-payload\n")
  }

  test("bare 412 is success when destination has this write owner") {
    val fs = new TestFileSystem
    fs.enqueueStream(closeFailure = Some(conditionalPreconditionFailure()))
    fs.readXAttr = (_, _) => fs.writeIds.head.getBytes(UTF_8)

    S3ConditionalWrite.write(fs, path, Iterator("payload").asJava)

    assert(fs.xAttrReads == 1)
  }

  test("bare 412 with a foreign owner is a conflict") {
    val fs = new TestFileSystem
    val closeFailure = conditionalPreconditionFailure()
    fs.enqueueStream(closeFailure = Some(closeFailure))
    fs.readXAttr = (_, _) => "foreign-write".getBytes(UTF_8)

    val error = intercept[NioFileAlreadyExistsException] {
      S3ConditionalWrite.write(fs, path, Iterator("payload").asJava)
    }

    assert(error.getCause eq closeFailure)
  }

  test("bare 412 with no visible destination preserves the original failure") {
    val fs = new TestFileSystem
    val closeFailure = conditionalPreconditionFailure()
    fs.enqueueStream(closeFailure = Some(closeFailure))
    fs.readXAttr = (_, _) => throw new FileNotFoundException(path.toString)

    val error = intercept[AWSServiceIOException] {
      S3ConditionalWrite.write(fs, path, Iterator("payload").asJava)
    }

    assert(error eq closeFailure)
    assert(fs.buildCount == 1)
  }

  test("409 with no destination opens a new stream and replays exact bytes with same owner") {
    val fs = new TestFileSystem
    fs.getConf.setInt(Constants.RETRY_LIMIT, 1)
    fs.getConf.set(Constants.RETRY_INTERVAL, "0ms")
    val first = fs.enqueueStream(closeFailure = Some(conditionalRequestConflict()))
    val second = fs.enqueueStream()
    fs.readXAttr = (_, _) => throw new FileNotFoundException(path.toString)

    S3ConditionalWrite.write(fs, path, Iterator("first", "second").asJava)

    assert(fs.buildCount == 2)
    assert(fs.writeIds.distinct.size == 1)
    assert(first.bytes.sameElements(second.bytes))
    assert(new String(second.bytes, UTF_8) == "first\nsecond\n")
  }

  test("409 replay stops at the configured retry limit") {
    val fs = new TestFileSystem
    fs.getConf.setInt(Constants.RETRY_LIMIT, 1)
    fs.getConf.set(Constants.RETRY_INTERVAL, "0ms")
    fs.enqueueStream(closeFailure = Some(conditionalRequestConflict()))
    fs.enqueueStream(closeFailure = Some(conditionalRequestConflict()))
    fs.readXAttr = (_, _) => throw new FileNotFoundException(path.toString)

    val error = intercept[AWSServiceIOException] {
      S3ConditionalWrite.write(fs, path, Iterator("payload").asJava)
    }

    assert(error.statusCode() == 409)
    assert(fs.buildCount == 2)
  }

  test("probe failure preserves the original ambiguous close failure") {
    val fs = new TestFileSystem
    val closeFailure = remoteFileChanged()
    val probeFailure = new IOException("HEAD failed")
    fs.enqueueStream(closeFailure = Some(closeFailure))
    fs.readXAttr = (_, _) => throw probeFailure

    val error = intercept[RemoteFileChangedException] {
      S3ConditionalWrite.write(fs, path, Iterator("payload").asJava)
    }

    assert(error eq closeFailure)
    assert(error.getSuppressed.contains(probeFailure))
  }

  test("iterator failure aborts without probing the destination") {
    val fs = new TestFileSystem
    val stream = fs.enqueueStream()
    val iteratorFailure = new IllegalStateException("iterator failed")
    val actions = new Iterator[String] {
      override def hasNext: Boolean = true
      override def next(): String = throw iteratorFailure
    }

    val error = intercept[IllegalStateException] {
      S3ConditionalWrite.write(fs, path, actions.asJava)
    }

    assert(error eq iteratorFailure)
    assert(stream.aborted)
    assert(fs.xAttrReads == 0)
  }

  test("stream write failure aborts and retains cleanup failure as suppressed") {
    val fs = new TestFileSystem
    val writeFailure = new IOException("write failed")
    val cleanupFailure = new IOException("abort failed")
    val stream = fs.enqueueStream(
      writeFailure = Some(writeFailure),
      abortFailure = Some(cleanupFailure))

    val error = intercept[IOException] {
      S3ConditionalWrite.write(fs, path, Iterator("payload").asJava)
    }

    assert(error eq writeFailure)
    assert(stream.aborted)
    assert(error.getSuppressed.contains(cleanupFailure))
    assert(fs.xAttrReads == 0)
  }

  test("replay failure aborts the replacement stream") {
    val fs = new TestFileSystem
    fs.getConf.setInt(Constants.RETRY_LIMIT, 1)
    fs.getConf.set(Constants.RETRY_INTERVAL, "0ms")
    fs.enqueueStream(closeFailure = Some(conditionalRequestConflict()))
    val replacementWriteFailure = new IOException("replacement write failed")
    val replacement = fs.enqueueStream(writeFailure = Some(replacementWriteFailure))
    fs.readXAttr = (_, _) => throw new FileNotFoundException(path.toString)

    val error = intercept[IOException] {
      S3ConditionalWrite.write(fs, path, Iterator("payload").asJava)
    }

    assert(error eq replacementWriteFailure)
    assert(replacement.aborted)
  }

  test("missing abort capability fails closed before writing") {
    val fs = new TestFileSystem
    val stream = fs.enqueueStream(abortable = false)

    intercept[UnsupportedOperationException] {
      S3ConditionalWrite.write(fs, path, Iterator("payload").asJava)
    }

    assert(stream.bytes.isEmpty)
    assert(fs.xAttrReads == 0)
  }

  private def remoteFileChanged(): RemoteFileChangedException =
    new RemoteFileChangedException(path.toString, "write", "precondition failed")

  private def conditionalRequestConflict(): AWSServiceIOException =
    new AWSServiceIOException(
      "complete multipart upload",
      S3Exception.builder().statusCode(409).message("ConditionalRequestConflict").build())

  private def conditionalPreconditionFailure(): AWSServiceIOException =
    new AWSServiceIOException(
      "conditional write",
      S3Exception.builder().statusCode(412).message("PreconditionFailed").build())

  private class TestFileSystem extends RawLocalFileSystem {
    private val streams = mutable.Queue.empty[ScriptedOutputStream]
    val mandatoryKeys = mutable.ArrayBuffer.empty[Set[String]]
    val options = mutable.ArrayBuffer.empty[Configuration]
    val writeIds = mutable.ArrayBuffer.empty[String]
    var buildCount = 0
    var xAttrReads = 0
    var readXAttr: (Path, String) => Array[Byte] =
      (_, _) => throw new FileNotFoundException(path.toString)

    setConf(new Configuration(false))

    def enqueueStream(
        closeFailure: Option[IOException] = None,
        writeFailure: Option[IOException] = None,
        abortFailure: Option[IOException] = None,
        abortable: Boolean = true): ScriptedOutputStream = {
      val stream =
        new ScriptedOutputStream(closeFailure, writeFailure, abortFailure, abortable)
      streams.enqueue(stream)
      stream
    }

    override def createFile(path: Path): FSDataOutputStreamBuilder[_, _] =
      new CapturingBuilder(this, path)

    override def getXAttr(path: Path, name: String): Array[Byte] = {
      xAttrReads += 1
      readXAttr(path, name)
    }

    private class CapturingBuilder(fs: FileSystem, path: Path)
      extends FSDataOutputStreamBuilder[FSDataOutputStream, CapturingBuilder](fs, path) {

      override def getThisBuilder: CapturingBuilder = this

      override def build(): FSDataOutputStream = {
        buildCount += 1
        mandatoryKeys += getMandatoryKeys.asScala.toSet
        options += new Configuration(getOptions)
        writeIds += getOptions.get(writeIdHeader)
        new FSDataOutputStream(streams.dequeue(), null)
      }
    }
  }

  private class ScriptedOutputStream(
      closeFailure: Option[IOException],
      writeFailure: Option[IOException],
      abortFailure: Option[IOException],
      abortable: Boolean)
    extends OutputStream
    with Abortable
    with StreamCapabilities {

    private val output = new ByteArrayOutputStream
    var aborted = false

    def bytes: Array[Byte] = output.toByteArray

    override def write(value: Int): Unit = {
      writeFailure.foreach(throw _)
      output.write(value)
    }

    override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = {
      writeFailure.foreach(throw _)
      output.write(bytes, offset, length)
    }

    override def close(): Unit = closeFailure.foreach(throw _)

    override def hasCapability(capability: String): Boolean =
      abortable && capability == StreamCapabilities.ABORTABLE_STREAM

    override def abort(): Abortable.AbortableResult = {
      aborted = true
      new Abortable.AbortableResult {
        override def alreadyClosed(): Boolean = false
        override def anyCleanupException(): IOException = abortFailure.orNull
      }
    }
  }
}
