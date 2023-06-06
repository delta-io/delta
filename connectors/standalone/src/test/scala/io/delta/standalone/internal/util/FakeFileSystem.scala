/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone.internal.util

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{DelegateToFileSystem, FsServerDefaults, RawLocalFileSystem}

/**
 * A fake file system that delegates the calls to local file system but uses a different scheme.
 * This can be used to test whether Hadoop configuration will be picked up.
 */
class FakeFileSystem extends RawLocalFileSystem {
  override def getScheme: String = FakeFileSystem.scheme
  override def getUri: URI = URI.create(s"$getScheme:///")
}

object FakeFileSystem {
  val scheme = "fake"

  def newConfiguration(): Configuration = {
    val conf = new Configuration()
    conf.set("fs.fake.impl", classOf[FakeFileSystem].getName)
    conf.set("fs.fake.impl.disable.cache", "true")
    conf.set("fs.AbstractFileSystem.fake.impl", classOf[FakeAbstractFileSystem].getName)
    conf
  }
}

/**
 * A fake AbstractFileSystem for [[FakeFileSystem]] to use the default log store. This is a wrapper
 * around [[FakeFileSystem]].
 */
class FakeAbstractFileSystem(uri: URI, conf: Configuration) extends DelegateToFileSystem(
    uri,
    new FakeFileSystem,
    conf,
    FakeFileSystem.scheme,
    false)
