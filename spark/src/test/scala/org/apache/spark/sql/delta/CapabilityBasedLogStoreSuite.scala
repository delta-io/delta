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

import java.io.{ByteArrayOutputStream, FileNotFoundException, IOException}
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta.storage.{DelegatingLogStore, LogStore, LogStoreAdaptor}
import io.delta.storage.integration.S3NativeTestFixture
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Abortable, FileStatus, FSDataInputStream, FSDataOutputStream, FSDataOutputStreamBuilder, FSInputStream, Path, StreamCapabilities}
import org.apache.hadoop.fs.Options.CreateFileOptionKeys.FS_OPTION_CREATE_CONDITIONAL_OVERWRITE
import org.apache.hadoop.fs.s3a.{Constants, S3AFileSystem}

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

/** Tests provider selection through public operations, with real S3A publication coverage. */
class CapabilityBasedLogStoreSuite extends QueryTest with SharedSparkSession {
  import CapabilityBasedLogStoreSuite._

  private def className(store: LogStore): String = store match {
    case adaptor: LogStoreAdaptor => adaptor.logStoreImpl.getClass.getName
    case other => other.getClass.getName
  }

  private def withFileSystem(f: (Configuration, State) => Unit): Unit = {
    val id = UUID.randomUUID().toString
    val state = new State
    states.synchronized { states(id) = state }
    val conf = new Configuration(false)
    conf.set(stateKey, id)
    Seq("s3", "s3a", "s3n").foreach { scheme =>
      conf.set(s"fs.$scheme.impl", classOf[CapabilityBasedS3AFileSystem].getName)
      conf.setBoolean(s"fs.$scheme.impl.disable.cache", true)
    }
    Utils.tryWithSafeFinally {
      f(conf, state)
    } {
      try state.instances.foreach(_.close()) finally {
        states.synchronized { states.remove(id) }
      }
    }
  }

  private def withSchemeOverride(key: String, value: String)(f: => Unit): Unit = {
    val conf = SparkEnv.get.conf
    val previous = conf.getOption(key)
    conf.set(key, value)
    try f finally {
      previous match {
        case Some(old) => conf.set(key, old)
        case None => conf.remove(key)
      }
    }
  }

  private def assertWrite(
      store: DelegatingLogStore,
      path: Path,
      conf: Configuration,
      state: State,
      native: Boolean): Unit = {
    val lines = Seq(s"commit for $path", "second action")
    store.write(path, lines.iterator, overwrite = false, conf)
    assert(state.writes.last.conditional == native)
    assert(state.writes.last.conf eq conf)
    assert(store.read(path, conf) == lines)
    assert(className(store.getDelegate(path, conf)) == (if (native) nativeClass else legacyClass))
  }

  test("default real S3A writes publish conditionally without a LogStore override") {
    val fixture = new S3NativeTestFixture
    try {
      val conf = fixture.configuration
      val store = new DelegatingLogStore(new Configuration(false))
      val path = fixture.path("auto/_delta_log/00000000000000000000.json")
      store.write(path, Iterator("first action", "second action"), overwrite = false, conf)
      assert(store.read(path, conf) == Seq("first action", "second action"))
      val puts = fixture.requests.filter(r =>
        r.operation == S3NativeTestFixture.Put && r.key == path.toUri.getPath.stripPrefix("/"))
      assert(puts.nonEmpty && puts.forall(_.conditional))
      assert(className(store.getDelegate(path, conf)) == nativeClass)
      assert(fixture.handlerErrors.isEmpty, fixture.handlerErrors.mkString("\n"))
      assert(fixture.pendingUploads == 0)
    } finally {
      fixture.close()
    }
  }

  for {
    scheme <- Seq("s3", "s3a", "s3n")
    missing <- Seq(None, Some(conditionalCapability), Some(metadataCapability))
  } {
    test(s"default $scheme selects and writes with missing capability $missing") {
      withFileSystem { (conf, state) =>
        missing.foreach(capability => conf.setBoolean(capabilityKey(capability), false))
        val store = new DelegatingLogStore(conf)
        assertWrite(store, new Path(s"$scheme://bucket/log.json"), conf, state,
          native = missing.isEmpty)
      }
    }
  }

  for {
    prefix <- Seq("spark.delta", "delta")
    global <- Seq(false, true)
    explicitClass <- Seq(legacyClass, nativeClass)
    failure <- Seq("initialize", "probe")
  } {
    val key = if (global) s"$prefix.logStore.class" else s"$prefix.logStore.s3a.impl"
    test(s"$key=$explicitClass skips filesystem $failure failure") {
      withFileSystem { (conf, state) =>
        conf.setBoolean(failInitializeKey, failure == "initialize")
        conf.setBoolean(failProbeKey, failure == "probe")
        val path = new Path("s3a://unavailable/log.json")
        def check(): Unit = {
          val created = LogStore.createLogStore(
            new SparkConf(false).set(key, explicitClass), conf)
          val selected = if (global) {
            assert(!created.isInstanceOf[DelegatingLogStore])
            created
          } else {
            created.asInstanceOf[DelegatingLogStore].getDelegate(path, conf)
          }
          assert(className(selected) == explicitClass)
          assert(state.initializations == 0, "an explicit override must not resolve the filesystem")
          assert(state.probes.isEmpty, "an explicit override must not probe capabilities")
        }
        if (global) check() else withSchemeOverride(key, explicitClass) { check() }
      }
    }
  }

  for (nativeFirst <- Seq(true, false)) {
    test(s"bucket decisions are recomputed while store instances are reused: $nativeFirst first") {
      withFileSystem { (conf, state) =>
        conf.setBoolean(bucketCapabilityKey("legacy", conditionalCapability), false)
        val store = new DelegatingLogStore(conf)
        val order = if (nativeFirst) Seq("native", "legacy") else Seq("legacy", "native")
        val selected = mutable.Map.empty[Boolean, LogStore]
        (order ++ order.reverse).zipWithIndex.foreach { case (bucket, index) =>
          val path = new Path(s"s3a://$bucket/$index.json")
          val native = bucket == "native"
          assertWrite(store, path, conf, state, native)
          val actual = store.getDelegate(path, conf)
          selected.get(native).foreach(previous => assert(previous eq actual))
          selected(native) = actual
        }
        assert(!(selected(true) eq selected(false)))
        assert(store.getDelegate(new Path("s3a://another-native/log.json"), conf) eq selected(true))
      }
    }

    test(s"operation configurations are re-evaluated on the same scheme: $nativeFirst first") {
      withFileSystem { (conf, state) =>
        val unsupported = new Configuration(conf)
        unsupported.setBoolean(capabilityKey(conditionalCapability), false)
        // A delegate's initialization config cannot replace an operation's config.
        val store = new DelegatingLogStore(if (nativeFirst) unsupported else conf)
        val order = if (nativeFirst) Seq(conf, unsupported) else Seq(unsupported, conf)
        val selected = mutable.Map.empty[Boolean, LogStore]
        (order ++ order.reverse).zipWithIndex.foreach { case (operationConf, index) =>
          val native = operationConf eq conf
          val path = new Path(s"s3a://same-bucket/$index.json")
          assertWrite(store, path, operationConf, state, native)
          val actual = store.getDelegate(path, operationConf)
          selected.get(native).foreach(previous => assert(previous eq actual))
          selected(native) = actual
        }
        assert(!(selected(true) eq selected(false)))
      }
    }
  }

  test("instance cache retains the scheme as well as the selected class") {
    withFileSystem { (conf, _) =>
      val store = new DelegatingLogStore(conf)
      val s3 = store.getDelegate(new Path("s3://bucket/first"), conf)
      val s3a = store.getDelegate(new Path("s3a://bucket/second"), conf)
      assert(className(s3) == nativeClass && className(s3a) == nativeClass)
      assert(!(s3 eq s3a))
      assert(store.getDelegate(new Path("s3://other/third"), conf) eq s3)
      assert(store.getDelegate(new Path("s3a://other/fourth"), conf) eq s3a)
    }
  }

  for {
    warmCache <- Seq(false, true)
    disableCache <- Seq(true, false)
  } {
    test(s"capability IOException propagates without fallback: warm cache=$warmCache, " +
        s"disable filesystem cache=$disableCache") {
      withFileSystem { (conf, state) =>
        conf.setBoolean("fs.s3a.impl.disable.cache", disableCache)
        val store = new DelegatingLogStore(conf)
        if (warmCache) {
          val unsupported = new Configuration(conf)
          unsupported.setBoolean(capabilityKey(conditionalCapability), false)
          assertWrite(store, new Path("s3a://bucket/earlier.json"), unsupported, state,
            native = false)
        }
        conf.setBoolean(failProbeKey, true)
        val path = new Path(s"s3a://failing-${UUID.randomUUID()}/log.json")
        val cached = if (disableCache) None else Some(path.getFileSystem(conf))
        val before = state.writes.size
        val error = intercept[IOException] {
          store.write(path, Iterator("must not publish"),
            overwrite = false, conf)
        }
        assert(error eq state.probeFailure)
        assert(state.writes.size == before)
        assert(!state.files.contains(path.toString))
        assert(state.instances.last.closed == disableCache,
          "failed probing must close only its uncached client before teardown")
        cached.foreach(fs => assert(path.getFileSystem(conf) eq fs))
      }
    }
  }

  test("probe IOException remains primary when closing its uncached filesystem also fails") {
    withFileSystem { (conf, state) =>
      conf.setBoolean(failProbeKey, true)
      conf.setBoolean(failCloseKey, true)
      val store = new DelegatingLogStore(conf)
      val path = new Path("s3a://probe-and-close-failure/log.json")
      val error = intercept[IOException] {
        store.write(path, Iterator("must not publish"), overwrite = false, conf)
      }
      assert(error eq state.probeFailure)
      assert(error.getSuppressed.toSeq == Seq(state.closeFailure))
      assert(state.instances.size == 1 && state.instances.head.closed)
      assert(state.writes.isEmpty && state.files.isEmpty)
    }
  }

  for (operation <- Seq("read", "write")) {
    test(s"config-less $operation selects and executes with one active-session configuration") {
      withFileSystem { (conf, state) =>
        val constructorConf = new Configuration(conf)
        constructorConf.setBoolean(capabilityKey(conditionalCapability), false)
        val store = new DelegatingLogStore(constructorConf)
        val path = new Path("s3a://session/log.json")
        state.files(path.toString) = "session action\n".getBytes(UTF_8)
        val settings = conf.iterator().asScala.map(e => e.getKey -> e.getValue).toSeq
        withSQLConf(settings: _*) {
          spark.withActive {
            if (operation == "read") {
              assert(store.read(path) == Seq("session action"))
            } else {
              state.files.clear()
              store.write(path, Iterator("session action"), overwrite = false)
              assert(state.writes.last.conditional)
              assert(new String(state.files(path.toString), UTF_8) == "session action\n")
            }
          }
        }
        assert(state.probes.nonEmpty, "selection must consult the active session's filesystem")
        val operationConf = if (operation == "read") state.reads.last else state.writes.last.conf
        assert(!(operationConf eq constructorConf))
        assert(state.probes.forall(_ eq operationConf),
          "selection and execution must share one Configuration, even with distinct filesystems")
      }
    }
  }

  for (disableCache <- Seq(true, false)) {
    test(s"selection closes only its uncached probe filesystem: disable cache=$disableCache") {
      withFileSystem { (conf, state) =>
        conf.setBoolean("fs.s3a.impl.disable.cache", disableCache)
        val path = new Path(s"s3a://lifecycle-${UUID.randomUUID()}/log.json")
        val cached = if (disableCache) None else Some(path.getFileSystem(conf))
        val store = new DelegatingLogStore(conf)
        assert(className(store.getDelegate(path, conf)) == nativeClass)
        assert(state.instances.size == 1)
        assert(state.instances.head.closed == disableCache)
        cached.foreach { fs =>
          assert(path.getFileSystem(conf) eq fs)
          store.write(path, Iterator("cached client remains usable"), overwrite = false, conf)
          assert(!state.instances.head.closed)
          assert(new String(state.files(path.toString), UTF_8) == "cached client remains usable\n")
        }
      }
    }
  }
}

private[delta] object CapabilityBasedLogStoreSuite {
  val nativeClass = "io.delta.storage.S3LogStore"
  val legacyClass = "io.delta.storage.S3SingleDriverLogStore"
  val conditionalCapability = FS_OPTION_CREATE_CONDITIONAL_OVERWRITE
  val metadataCapability = Constants.FS_S3A_CREATE_HEADER
  val stateKey = "delta.test.capability.state"
  val failInitializeKey = "delta.test.capability.failInitialize"
  val failProbeKey = "delta.test.capability.failProbe"
  val failCloseKey = "delta.test.capability.failClose"
  def capabilityKey(capability: String): String = s"delta.test.capability.$capability"
  def bucketCapabilityKey(bucket: String, capability: String): String =
    s"delta.test.capability.bucket.$bucket.$capability"

  case class Write(conf: Configuration, conditional: Boolean)
  class State {
    val files = mutable.Map.empty[String, Array[Byte]]
    val probes = mutable.ArrayBuffer.empty[Configuration]
    val reads = mutable.ArrayBuffer.empty[Configuration]
    val writes = mutable.ArrayBuffer.empty[Write]
    val instances = mutable.ArrayBuffer.empty[CapabilityBasedS3AFileSystem]
    val probeFailure = new IOException("capability probe unavailable")
    val closeFailure = new IOException("probe client close failed")
    var initializations = 0
  }
  val states = mutable.Map.empty[String, State]
}

/** Hadoop instantiates this by name, with its cache disabled for configuration-sensitive tests. */
class CapabilityBasedS3AFileSystem extends S3AFileSystem {
  import CapabilityBasedLogStoreSuite._

  private var uri: URI = _
  private var state: State = _
  var closed = false

  override def initialize(name: URI, conf: Configuration): Unit = {
    setConf(conf)
    state = states.synchronized { states(conf.get(stateKey)) }
    state.initializations += 1
    if (conf.getBoolean(failInitializeKey, false)) throw new IOException("filesystem unavailable")
    uri = URI.create(s"${name.getScheme}://${name.getAuthority}")
    state.instances += this
  }

  override def getUri: URI = uri
  override def makeQualified(path: Path): Path = path.makeQualified(uri, new Path("/"))
  override def getDefaultBlockSize: Long = 4096L
  override def close(): Unit = {
    // Also evict an explicitly closed cached instance from Hadoop's cache.
    val firstClose = !closed
    closed = true
    super.close()
    if (firstClose && getConf.getBoolean(failCloseKey, false)) throw state.closeFailure
  }

  // This subclass never starts the S3A client services; retain only FileSystem cache cleanup.
  override protected def stopAllServices(): Unit = {}

  private def requireOpen(): Unit = {
    if (closed) throw new IOException("filesystem was closed during selection")
  }

  override def hasPathCapability(path: Path, capability: String): Boolean = {
    requireOpen()
    state.probes += getConf
    if (getConf.getBoolean(failProbeKey, false)) throw state.probeFailure
    Set(conditionalCapability, metadataCapability).contains(capability) &&
      getConf.getBoolean(bucketCapabilityKey(path.toUri.getAuthority, capability),
        getConf.getBoolean(capabilityKey(capability), true))
  }

  override def getFileStatus(path: Path): FileStatus = {
    requireOpen()
    val bytes = state.files.getOrElse(path.toString, throw new FileNotFoundException(path.toString))
    new FileStatus(bytes.length, false, 1, 4096, 0, path)
  }

  override def exists(path: Path): Boolean = {
    requireOpen()
    state.files.contains(path.toString)
  }

  override def open(path: Path, bufferSize: Int): FSDataInputStream = {
    requireOpen()
    state.reads += getConf
    val bytes = state.files.getOrElse(path.toString, throw new FileNotFoundException(path.toString))
    new FSDataInputStream(new FSInputStream {
      private var position = 0
      override def read(): Int = {
        if (position >= bytes.length) -1 else {
          val value = bytes(position) & 0xff
          position += 1
          value
        }
      }
      override def seek(offset: Long): Unit = { position = offset.toInt }
      override def getPos: Long = position
      override def seekToNewSource(offset: Long): Boolean = false
    })
  }

  override def create(path: Path, overwrite: Boolean): FSDataOutputStream =
    output(path, overwrite, conditional = false)

  override def createFile(path: Path): FSDataOutputStreamBuilder[_, _] = new Builder(path)

  private class Builder(path: Path)
    extends FSDataOutputStreamBuilder[FSDataOutputStream, Builder](
      CapabilityBasedS3AFileSystem.this, path) {
    override def getThisBuilder: Builder = this
    override def build(): FSDataOutputStream = {
      val conditional = getOptions.getBoolean(conditionalCapability, false)
      val overwrite = getFlags.contains(org.apache.hadoop.fs.CreateFlag.OVERWRITE)
      output(path, overwrite, conditional)
    }
  }

  private def output(path: Path, overwrite: Boolean, conditional: Boolean): FSDataOutputStream = {
    requireOpen()
    val conf = getConf
    val stream = new ByteArrayOutputStream with Abortable with StreamCapabilities {
      private var aborted = false
      override def hasCapability(capability: String): Boolean =
        capability == StreamCapabilities.ABORTABLE_STREAM ||
          (capability == conditionalCapability && conditional)

      override def close(): Unit = {
        requireOpen()
        if (!aborted) {
          if (!overwrite && state.files.contains(path.toString)) {
            throw new org.apache.hadoop.fs.FileAlreadyExistsException(path.toString)
          }
          state.files(path.toString) = toByteArray
          state.writes += Write(conf, conditional)
        }
      }

      override def abort(): Abortable.AbortableResult = {
        aborted = true
        new Abortable.AbortableResult {
          override def alreadyClosed(): Boolean = false
          override def anyCleanupException(): IOException = null
        }
      }
    }
    new FSDataOutputStream(stream, null)
  }
}
