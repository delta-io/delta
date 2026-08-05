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

import java.io.{ByteArrayOutputStream, FileNotFoundException, IOException, OutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{FileSystems, Files}
import java.nio.file.attribute.PosixFilePermissions
import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.s3a.{AWSBadRequestException, RemoteFileChangedException}
import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.services.s3.model.S3Exception

class S3ConditionalWriteSuite extends AnyFunSuite {
  private val path = new Path("/table/_delta_log/00000000000000000001.json")

  test("conditional write uses mandatory create and owner metadata options") {
    val fs = new RecordingFileSystem

    S3ConditionalWrite.write(fs, path, Seq("one", "two").iterator.asJava, false)

    assert(fs.createFileCount === 1)
    assert(fs.regularCreateCount === 0)
    assert(fs.createFlag)
    assert(!fs.overwriteFlag)
    assert(fs.mandatoryKeys === Set(
      S3ConditionalWrite.CONDITIONAL_CREATE_OPTION,
      S3ConditionalWrite.WRITE_ID_HEADER_OPTION))
    assert(fs.option(S3ConditionalWrite.CONDITIONAL_CREATE_OPTION) === "true")
    assert(UUID.fromString(fs.option(S3ConditionalWrite.WRITE_ID_HEADER_OPTION)).toString ===
      fs.option(S3ConditionalWrite.WRITE_ID_HEADER_OPTION))
  }

  test("conditional write emits UTF-8 actions with one newline per action") {
    val fs = new RecordingFileSystem

    S3ConditionalWrite.write(fs, path, Seq("one", "♥").iterator.asJava, false)

    assert(new String(fs.lastStream.bytes, StandardCharsets.UTF_8) === "one\n♥\n")
    assert(fs.lastStream.closeCount === 1)
    assert(fs.lastStream.abortCount === 0)
  }

  test("iterator failure aborts without closing the conditional stream") {
    val fs = new RecordingFileSystem
    val failure = new IllegalStateException("iterator failed")
    val actions = new Iterator[String] {
      override def hasNext: Boolean = throw failure
      override def next(): String = throw failure
    }

    val thrown = intercept[IllegalStateException] {
      S3ConditionalWrite.write(fs, path, actions.asJava, false)
    }

    assert(thrown eq failure)
    assert(fs.lastStream.abortCount === 1)
    assert(fs.lastStream.closeCount === 0)
  }

  test("stream write failure aborts without closing the conditional stream") {
    val fs = new RecordingFileSystem
    val failure = new IOException("write failed")
    fs.writeFailure = Some(failure)

    val thrown = intercept[IOException] {
      S3ConditionalWrite.write(fs, path, Iterator("one").asJava, false)
    }

    assert(thrown eq failure)
    assert(fs.lastStream.abortCount === 1)
    assert(fs.lastStream.closeCount === 0)
  }

  test("conditional conflicts from another writer become NIO FileAlreadyExistsException") {
    Seq(
      remoteFileChanged(),
      new org.apache.hadoop.fs.FileAlreadyExistsException(path.toString)
    ).foreach { closeFailure =>
      val fs = new RecordingFileSystem
      fs.ownerOnClose = _ => Some("another-writer")
      fs.closeFailure = Some(closeFailure)

      val thrown = intercept[java.nio.file.FileAlreadyExistsException] {
        S3ConditionalWrite.write(fs, path, Iterator("one").asJava, false)
      }

      assert(thrown.getFile === path.toString)
      assert(thrown.getCause eq closeFailure)
    }
  }

  test("conditional conflict is successful when HEAD finds this write's owner token") {
    val fs = new RecordingFileSystem
    fs.ownerOnClose = owner => Some(owner)
    fs.closeFailure = Some(remoteFileChanged())

    S3ConditionalWrite.write(fs, path, Iterator("one").asJava, false)

    assert(fs.getXAttrCount === 1)
  }

  test("builder failure is never reconciled as a successful upload") {
    val fs = new RecordingFileSystem
    fs.ownerOnBuild = owner => Some(owner)
    val buildFailure = remoteFileChanged()
    fs.buildFailure = Some(buildFailure)

    val thrown = intercept[RemoteFileChangedException] {
      S3ConditionalWrite.write(fs, path, Iterator("one").asJava, false)
    }

    assert(thrown eq buildFailure)
    assert(fs.lastStream === null)
    assert(fs.getXAttrCount === 0)
  }

  test("failed HEAD preserves an ambiguous close failure") {
    val fs = new RecordingFileSystem
    val closeFailure = remoteFileChanged()
    val headFailure = new IOException("HEAD failed")
    fs.closeFailure = Some(closeFailure)
    fs.getXAttrFailure = Some(headFailure)

    val thrown = intercept[RemoteFileChangedException] {
      S3ConditionalWrite.write(fs, path, Iterator("one").asJava, false)
    }

    assert(thrown eq closeFailure)
    assert(thrown.getSuppressed.toSeq.contains(headFailure))
  }

  test("409 with a foreign owner is reported as FileAlreadyExistsException") {
    val fs = new RecordingFileSystem
    val closeFailure = conditionalRequestConflict()
    fs.closeFailure = Some(closeFailure)
    fs.ownerOnClose = _ => Some("another-writer")

    val thrown = intercept[java.nio.file.FileAlreadyExistsException] {
      S3ConditionalWrite.write(fs, path, Iterator("one").asJava, false)
    }

    assert(thrown.getFile === path.toString)
    assert(thrown.getCause eq closeFailure)
  }

  test("409 with no destination retries the complete write with the same owner token") {
    val fs = new RecordingFileSystem
    val closeFailure = conditionalRequestConflict()
    val headFailure = new FileNotFoundException(path.toString)
    fs.getConf.setInt("fs.s3a.retry.limit", 1)
    fs.getConf.set("fs.s3a.retry.interval", "0ms")
    fs.closeFailureForAttempt = attempt => if (attempt == 1) Some(closeFailure) else None
    fs.getXAttrFailureForAttempt = attempt => if (attempt == 1) Some(headFailure) else None
    fs.ownerOnCloseForAttempt = (owner, attempt) => if (attempt == 2) Some(owner) else None

    S3ConditionalWrite.write(fs, path, Iterator("one", "♥").asJava, false)

    assert(fs.createFileCount === 2)
    assert(fs.getXAttrCount === 1)
    assert(fs.writeIds.distinct.size === 1)
    assert(fs.streams.map(stream => new String(stream.bytes, StandardCharsets.UTF_8)) ===
      Seq("one\n♥\n", "one\n♥\n"))
  }

  test("repeated 409 responses stop at the configured S3A retry limit") {
    val fs = new RecordingFileSystem
    fs.getConf.setInt("fs.s3a.retry.limit", 2)
    fs.getConf.set("fs.s3a.retry.interval", "0ms")
    fs.closeFailureForAttempt = _ => Some(conditionalRequestConflict())
    fs.getXAttrFailureForAttempt = _ => Some(new FileNotFoundException(path.toString))

    val thrown = intercept[AWSBadRequestException] {
      S3ConditionalWrite.write(fs, path, Iterator("one").asJava, false)
    }

    assert(thrown.statusCode() === 409)
    assert(fs.createFileCount === 3)
    assert(fs.getXAttrCount === 3)
    assert(fs.writeIds.distinct.size === 1)
    assert(fs.streams.map(stream => new String(stream.bytes, StandardCharsets.UTF_8)) ===
      Seq.fill(3)("one\n"))
  }

  test("replay buffer spills with owner-only permissions and deletes the spill file") {
    val replayBuffer = new S3WriteReplayBuffer(4)
    var spillFile: java.nio.file.Path = null
    try {
      replayBuffer.write("ab".getBytes(StandardCharsets.UTF_8))
      assert(!replayBuffer.hasSpilledToDisk)
      replayBuffer.write("cde".getBytes(StandardCharsets.UTF_8))
      assert(replayBuffer.hasSpilledToDisk)

      spillFile = replayBuffer.spillFile()
      assert(Files.isRegularFile(spillFile))
      if (FileSystems.getDefault.supportedFileAttributeViews().contains("posix")) {
        assert(Files.getPosixFilePermissions(spillFile) ===
          PosixFilePermissions.fromString("rw-------"))
      }

      val replayed = new ByteArrayOutputStream
      replayBuffer.seal()
      replayBuffer.replayTo(replayed)
      assert(new String(replayed.toByteArray, StandardCharsets.UTF_8) === "abcde")
    } finally {
      replayBuffer.close()
    }
    assert(!Files.exists(spillFile))
  }

  test("non-conditional close failure is not hidden by a foreign owner") {
    val fs = new RecordingFileSystem
    val closeFailure = new IOException("connection failed")
    fs.closeFailure = Some(closeFailure)
    fs.ownerOnClose = _ => Some("another-writer")

    val thrown = intercept[IOException] {
      S3ConditionalWrite.write(fs, path, Iterator("one").asJava, false)
    }

    assert(thrown eq closeFailure)
  }

  test("unsupported mandatory create options fail closed") {
    val fs = new RecordingFileSystem
    fs.supportsMandatoryOptions = false

    assertThrows[IllegalArgumentException] {
      S3ConditionalWrite.write(fs, path, Iterator("one").asJava, false)
    }
    assert(fs.lastStream === null)
    assert(fs.regularCreateCount === 0)
  }

  test("a stream without abort support fails before consuming actions") {
    val fs = new RecordingFileSystem
    fs.abortable = false
    var iteratorConsumed = false
    val actions = Iterator.continually {
      iteratorConsumed = true
      "one"
    }.take(1)

    assertThrows[UnsupportedOperationException] {
      S3ConditionalWrite.write(fs, path, actions.asJava, false)
    }

    assert(!iteratorConsumed)
    assert(fs.lastStream.closeCount === 0)
  }

  test("overwrite uses the ordinary FileSystem create path") {
    val fs = new RecordingFileSystem

    S3ConditionalWrite.write(fs, path, Iterator("replacement").asJava, true)

    assert(fs.createFileCount === 0)
    assert(fs.regularCreateCount === 1)
    assert(new String(fs.lastStream.bytes, StandardCharsets.UTF_8) === "replacement\n")
  }

  private def remoteFileChanged(): RemoteFileChangedException =
    new RemoteFileChangedException(path.toString, "close", "precondition failed")

  private def conditionalRequestConflict(): AWSBadRequestException =
    new AWSBadRequestException(
      path.toString,
      S3Exception.builder().statusCode(409).message("ConditionalRequestConflict").build())
}

private class RecordingFileSystem extends RawLocalFileSystem {
  setConf(new Configuration(false))

  var createFileCount = 0
  var regularCreateCount = 0
  var createFlag = false
  var overwriteFlag = false
  var supportsMandatoryOptions = true
  var abortable = true
  var mandatoryKeys = Set.empty[String]
  var options = Map.empty[String, String]
  var lastStream: RecordingOutputStream = _
  var writeFailure = Option.empty[IOException]
  var buildFailure = Option.empty[IOException]
  var closeFailure = Option.empty[IOException]
  var getXAttrFailure = Option.empty[IOException]
  var ownerOnBuild: String => Option[String] = _ => None
  var ownerOnClose: String => Option[String] = _ => None
  var closeFailureForAttempt: Int => Option[IOException] = _ => closeFailure
  var getXAttrFailureForAttempt: Int => Option[IOException] = _ => getXAttrFailure
  var ownerOnCloseForAttempt: (String, Int) => Option[String] =
    (owner, _) => ownerOnClose(owner)
  var storedOwner = Option.empty[String]
  var getXAttrCount = 0
  var closeAttemptCount = 0
  var writeIds = Seq.empty[String]
  var streams = Seq.empty[RecordingOutputStream]

  def option(key: String): String = options(key)

  override def createFile(path: Path): FSDataOutputStreamBuilder[_, _] = {
    createFileCount += 1
    new RecordingBuilder(this, path).create().overwrite(true)
  }

  override def create(path: Path, overwrite: Boolean): FSDataOutputStream = {
    regularCreateCount += 1
    newStream()
  }

  override def getXAttr(path: Path, name: String): Array[Byte] = {
    getXAttrCount += 1
    getXAttrFailureForAttempt(getXAttrCount).foreach(throw _)
    if (name == S3ConditionalWrite.WRITE_ID_XATTR) {
      storedOwner.map(_.getBytes(StandardCharsets.UTF_8)).orNull
    } else {
      null
    }
  }

  def newStream(): FSDataOutputStream = {
    lastStream = new RecordingOutputStream(this)
    streams :+= lastStream
    new FSDataOutputStream(lastStream, null)
  }
}

private class RecordingBuilder(fs: RecordingFileSystem, path: Path)
  extends FSDataOutputStreamBuilder[FSDataOutputStream, RecordingBuilder](fs, path) {

  override def getThisBuilder: RecordingBuilder = this

  override def build(): FSDataOutputStream = {
    fs.mandatoryKeys = getMandatoryKeys.asScala.toSet
    fs.options = getOptions.iterator().asScala.map(entry => entry.getKey -> entry.getValue).toMap
    fs.writeIds :+= fs.options.getOrElse(S3ConditionalWrite.WRITE_ID_HEADER_OPTION, "")
    fs.createFlag = getFlags.contains(CreateFlag.CREATE)
    fs.overwriteFlag = getFlags.contains(CreateFlag.OVERWRITE)
    if (!fs.supportsMandatoryOptions) {
      throw new IllegalArgumentException("mandatory options unsupported")
    }
    fs.buildFailure.foreach { failure =>
      val writeId = fs.options.getOrElse(S3ConditionalWrite.WRITE_ID_HEADER_OPTION, "")
      fs.storedOwner = fs.ownerOnBuild(writeId)
      throw failure
    }
    fs.newStream()
  }
}

private class RecordingOutputStream(fs: RecordingFileSystem)
  extends OutputStream with Abortable with StreamCapabilities {

  private val buffer = new ByteArrayOutputStream
  var abortCount = 0
  var closeCount = 0

  def bytes: Array[Byte] = buffer.toByteArray

  override def write(value: Int): Unit = {
    fs.writeFailure.foreach(throw _)
    buffer.write(value)
  }

  override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = {
    fs.writeFailure.foreach(throw _)
    buffer.write(bytes, offset, length)
  }

  override def close(): Unit = {
    closeCount += 1
    fs.closeAttemptCount += 1
    val writeId = fs.options.getOrElse(S3ConditionalWrite.WRITE_ID_HEADER_OPTION, "")
    fs.storedOwner = fs.ownerOnCloseForAttempt(writeId, fs.closeAttemptCount)
    fs.closeFailureForAttempt(fs.closeAttemptCount).foreach(throw _)
  }

  override def abort(): Abortable.AbortableResult = {
    abortCount += 1
    if (!fs.abortable) {
      throw new UnsupportedOperationException("abort unsupported")
    }
    new Abortable.AbortableResult {
      override def alreadyClosed(): Boolean = false
      override def anyCleanupException(): IOException = null
    }
  }

  override def hasCapability(capability: String): Boolean =
    fs.abortable && capability == StreamCapabilities.ABORTABLE_STREAM
}
