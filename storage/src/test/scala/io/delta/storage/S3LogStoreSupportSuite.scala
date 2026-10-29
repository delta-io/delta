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

import java.lang.reflect.InvocationTargetException
import java.net.URLClassLoader
import java.nio.file.Files
import java.util.Collections

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, RawLocalFileSystem}
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
        val clazz = Option(findLoadedClass(name)).getOrElse(findClass(name))
        loaded += name
        if (resolve) resolveClass(clazz)
        clazz
      } else {
        super.loadClass(name, resolve)
      }
    }
  }
}
