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

package io.delta.storage

import java.io.{ByteArrayOutputStream, IOException, OutputStream}
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Options.CreateFileOptionKeys.FS_OPTION_CREATE_CONDITIONAL_OVERWRITE
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.scalatest.funsuite.AnyFunSuite

class S3LogStoreSuite extends AnyFunSuite {

  test("no-overwrite writes use conditional create") {
    val conf = testConfiguration()
    val store = new S3LogStore(conf)
    val path = new Path(s"${TestS3FileSystem.scheme}:///delta/00000000000000000001.json")

    store.write(path, Iterator("first", "second").asJava, false, conf)

    val fs = TestS3FileSystem.latest
    assert(fs.conditionalBuilds == 1)
    assert(fs.classicOverwrites.isEmpty)
    assert(fs.lastMandatoryKeys.contains(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE))
    assert(new String(fs.lastBytes, UTF_8) == "first\nsecond\n")
  }

  test("overwrite writes retain the existing S3SingleDriver path") {
    val conf = testConfiguration()
    val store = new S3LogStore(conf)
    val path = new Path(s"${TestS3FileSystem.scheme}:///delta/_last_checkpoint")

    store.write(path, Iterator("checkpoint").asJava, true, conf)

    val fs = TestS3FileSystem.latest
    assert(fs.conditionalBuilds == 0)
    assert(fs.classicOverwrites == Seq(true))
    assert(new String(fs.lastBytes, UTF_8) == "checkpoint\n")
  }

  private def testConfiguration(): Configuration = {
    TestS3FileSystem.reset()
    val conf = new Configuration(false)
    conf.set(s"fs.${TestS3FileSystem.scheme}.impl", classOf[TestS3FileSystem].getName)
    conf.setBoolean(s"fs.${TestS3FileSystem.scheme}.impl.disable.cache", true)
    conf
  }
}

class TestS3FileSystem extends RawLocalFileSystem {
  import TestS3FileSystem._

  latest = this

  var conditionalBuilds = 0
  var classicOverwrites = Seq.empty[Boolean]
  var lastMandatoryKeys = Set.empty[String]
  var lastBytes = Array.emptyByteArray

  override def getScheme: String = scheme
  override def getUri: URI = uri

  override def createFile(path: Path): FSDataOutputStreamBuilder[_, _] =
    new CapturingBuilder(this, path)

  override def create(path: Path, overwrite: Boolean): FSDataOutputStream = {
    classicOverwrites :+= overwrite
    outputStream()
  }

  override def create(
      path: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    classicOverwrites :+= overwrite
    outputStream()
  }

  private def outputStream(): FSDataOutputStream = {
    val output = new CapturingOutputStream(bytes => lastBytes = bytes)
    new FSDataOutputStream(output, null)
  }

  private class CapturingBuilder(fs: FileSystem, path: Path)
    extends FSDataOutputStreamBuilder[FSDataOutputStream, CapturingBuilder](fs, path) {

    override def getThisBuilder: CapturingBuilder = this

    override def build(): FSDataOutputStream = {
      conditionalBuilds += 1
      lastMandatoryKeys = getMandatoryKeys.asScala.toSet
      outputStream()
    }
  }
}

object TestS3FileSystem {
  val scheme = "test-s3"
  val uri: URI = URI.create(s"$scheme:///")

  @volatile var latest: TestS3FileSystem = _

  def reset(): Unit = latest = null
}

private class CapturingOutputStream(onClose: Array[Byte] => Unit)
  extends OutputStream
  with Abortable
  with StreamCapabilities {

  private val output = new ByteArrayOutputStream

  override def write(value: Int): Unit = output.write(value)

  override def write(bytes: Array[Byte], offset: Int, length: Int): Unit =
    output.write(bytes, offset, length)

  override def close(): Unit = onClose(output.toByteArray)

  override def hasCapability(capability: String): Boolean =
    capability == StreamCapabilities.ABORTABLE_STREAM

  override def abort(): Abortable.AbortableResult =
    new Abortable.AbortableResult {
      override def alreadyClosed(): Boolean = false
      override def anyCleanupException(): IOException = null
    }
}
