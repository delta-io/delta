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

import java.io.IOException
import java.lang.reflect.InvocationTargetException
import java.net.URLClassLoader
import java.nio.file.Files
import java.util.Collections

import scala.collection.mutable

import io.delta.storage.integration.S3NativeTestFixture
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, RawLocalFileSystem}
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.scalatest.funsuite.AnyFunSuite

class S3LogStoreSupportSuite extends AnyFunSuite {
  test("missing optional hadoop-aws does not prevent legacy selection or writing") {
    val loader = new IsolatedStorageLoader(missingAws = true)
    val fs = new RawLocalFileSystem
    val conf = new Configuration(false)
    conf.set("fs.file.impl", classOf[RawLocalFileSystem].getName)
    fs.initialize(new java.net.URI("file:///"), conf)
    val directory = Files.createTempDirectory("s3-support-")
    val path = new Path(directory.resolve("log.json").toUri)
    try {
      val nativeClass = loader.loadClass("io.delta.storage.S3LogStore")
      val supported = nativeClass.getMethod("isSupported", classOf[FileSystem], classOf[Path])
      assert(supported.invoke(null, fs, path) == java.lang.Boolean.FALSE)
      val legacy = loader.loadClass("io.delta.storage.S3SingleDriverLogStore")
        .getConstructor(classOf[Configuration]).newInstance(conf)
      legacy.getClass.getMethod("write", classOf[Path], classOf[java.util.Iterator[_]],
        classOf[java.lang.Boolean], classOf[Configuration])
        .invoke(legacy, path, Collections.singletonList("action").iterator(),
          java.lang.Boolean.FALSE, conf)
      assert(Files.readString(directory.resolve("log.json")) == "action\n")
      assert(!loader.loaded.contains("io.delta.storage.internal.S3ConditionalWrite"))
    } finally {
      fs.delete(new Path(directory.toUri), true)
      fs.close()
      loader.close()
    }
  }

  test("broken S3A dependencies propagate instead of silently selecting legacy writes") {
    val loader = new IsolatedStorageLoader(missingAws = false)
    try {
      val nativeClass = loader.loadClass("io.delta.storage.S3LogStore")
      val error = intercept[InvocationTargetException] {
        nativeClass.getMethod("isSupported", classOf[FileSystem], classOf[Path])
          .invoke(null, new RawLocalFileSystem, new Path("file:///log.json"))
      }
      assert(error.getCause.isInstanceOf[NoClassDefFoundError])
      assert(error.getCause.getMessage == "injected/missing/SdkDependency")
      assert(!loader.loaded.contains("io.delta.storage.internal.S3ConditionalWrite"))
    } finally {
      loader.close()
    }
  }

  for (parentHasS3A <- Seq(false, true)) {
    val layout = if (parentHasS3A) "a separate S3A copy" else "no S3A classes"

    test(s"capable child-loaded S3A rejects writes when Delta's loader has $layout") {
      withChildLoadedS3A(parentHasS3A, conditionalEnabled = true) { (nativeClass, fs, path) =>
        assert(fs.hasPathCapability(path, "fs.option.create.conditional.overwrite"))
        assert(fs.hasPathCapability(path, "fs.s3a.create.header"))
        assert(nativeClass.getMethod("hasNativeWriteCapabilities",
          classOf[FileSystem], classOf[Path]).invoke(null, fs, path) == java.lang.Boolean.TRUE)
        val error = intercept[IOException](checkSupport(nativeClass, fs, path))
        assert(error.getMessage.contains("classloader"))

        val store = nativeClass.getConstructor(classOf[Configuration]).newInstance(fs.getConf)
        val actions = new java.util.Iterator[String] {
          override def hasNext: Boolean = fail("Classloader failure must not consume actions")
          override def next(): String = fail("Classloader failure must not consume actions")
        }
        val writeError = intercept[InvocationTargetException] {
          nativeClass.getMethod("write", classOf[Path], classOf[java.util.Iterator[_]],
            classOf[java.lang.Boolean], classOf[Configuration])
            .invoke(store, path, actions, java.lang.Boolean.FALSE, fs.getConf)
        }
        assert(writeError.getCause.isInstanceOf[IOException])
        assert(writeError.getCause.getMessage.contains("classloader"))
      }
    }

    test(s"disabled conditional writes still allow fallback when Delta's loader has $layout") {
      withChildLoadedS3A(parentHasS3A, conditionalEnabled = false) { (nativeClass, fs, path) =>
        assert(!fs.hasPathCapability(path, "fs.option.create.conditional.overwrite"))
        assert(!checkSupport(nativeClass, fs, path))
      }
    }
  }

  private def checkSupport(nativeClass: Class[_], fs: FileSystem, path: Path): Boolean = {
    try {
      nativeClass.getMethod("isSupported", classOf[FileSystem], classOf[Path])
        .invoke(null, fs, path).asInstanceOf[java.lang.Boolean].booleanValue()
    } catch {
      case error: InvocationTargetException => throw error.getCause
    }
  }

  private def withChildLoadedS3A(parentHasS3A: Boolean, conditionalEnabled: Boolean)(
      f: (Class[_], FileSystem, Path) => Unit): Unit = {
    val fixture = new S3NativeTestFixture
    val storageLoader = new IsolatedStorageLoader(missingAws = true)
    val parent: ClassLoader =
      if (parentHasS3A) classOf[S3LogStore].getClassLoader else storageLoader
    val child = new ChildS3ALoader(parent)
    var fs: FileSystem = null
    try {
      val conf = fixture.configuration
      conf.setClassLoader(child)
      conf.set("fs.s3a.impl", classOf[ChildLoadedS3AFileSystem].getName)
      conf.setBoolean("fs.s3a.impl.disable.cache", false)
      conf.setBoolean("fs.s3a.create.conditional.enabled", conditionalEnabled)
      val path = fixture.path("classloader/_delta_log/00000000000000000000.json")
      fs = path.getFileSystem(conf)
      assert(fs.getClass.getClassLoader eq child)
      assert(fs.getClass.getSuperclass.getName == classOf[S3AFileSystem].getName)
      assert(fs.getClass.getSuperclass ne classOf[S3AFileSystem])
      val nativeClass: Class[_] = if (parentHasS3A) classOf[S3LogStore]
        else storageLoader.loadClass(classOf[S3LogStore].getName)
      f(nativeClass, fs, path)
      assert(!storageLoader.loaded.contains("io.delta.storage.internal.S3ConditionalWrite"))
      assert(fixture.requests.forall(r =>
        r.operation != S3NativeTestFixture.Put && r.operation != S3NativeTestFixture.Initiate))
    } finally {
      try {
        if (fs != null) fs.close()
      } finally {
        try fixture.close() finally {
          try child.close() finally storageLoader.close()
        }
      }
    }
  }

  private class IsolatedStorageLoader(missingAws: Boolean)
    extends URLClassLoader(Array(classOf[S3LogStore].getProtectionDomain.getCodeSource.getLocation),
      classOf[S3LogStore].getClassLoader) {
    val loaded = mutable.Set.empty[String]

    override protected def loadClass(name: String, resolve: Boolean): Class[_] = synchronized {
      if (name == "org.apache.hadoop.fs.s3a.S3AFileSystem") {
        if (missingAws) throw new ClassNotFoundException(name)
        throw new NoClassDefFoundError("injected/missing/SdkDependency")
      }
      if (name.startsWith("io.delta.storage.")) {
        val clazz: Class[_] = Option(findLoadedClass(name)).getOrElse(findClass(name))
        loaded += name
        if (resolve) resolveClass(clazz)
        clazz
      } else {
        super.loadClass(name, resolve)
      }
    }
  }
}
