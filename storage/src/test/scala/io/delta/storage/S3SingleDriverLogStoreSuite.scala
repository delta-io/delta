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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, RawLocalFileSystem}
import org.scalatest.funsuite.AnyFunSuite

class S3SingleDriverLogStoreSuite extends AnyFunSuite {
  test("S3SingleDriverLogStore preserves subclasses with an independent resolvePath helper") {
    val path = new Path("/table/_delta_log/00000000000000000001.json")
    val subclass = new HistoricalResolvePathSubclass(new Configuration(false))

    assert(subclass.callResolvePath(new RawLocalFileSystem, path) eq path)
  }
}

/**
 * Models a subclass compiled when S3SingleDriverLogStore.resolvePath was private.
 *
 * A private superclass helper does not reserve the signature, so widening it to final would break
 * existing subclasses even though they never depended on the helper.
 */
private class HistoricalResolvePathSubclass(configuration: Configuration)
  extends S3SingleDriverLogStore(configuration) {

  protected def resolvePath(fileSystem: FileSystem, path: Path): Path = path

  def callResolvePath(fileSystem: FileSystem, path: Path): Path =
    resolvePath(fileSystem, path)
}
