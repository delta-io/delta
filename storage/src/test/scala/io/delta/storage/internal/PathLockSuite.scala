/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

class PathLockSuite extends AnyFunSuite {

  private val path = new Path("s3://bucket/_delta_log/00000000000000000000.json")

  test("acquire then release round-trips and can be re-acquired") {
    val pathLock = new PathLock()
    pathLock.acquire(path)
    pathLock.release(path)
    // The path is free again, so a second acquire must not block.
    pathLock.acquire(path)
    pathLock.release(path)
  }

  test("release is a no-op when the path was never acquired") {
    val pathLock = new PathLock()
    // Previously this threw a NullPointerException from `synchronized(null)` because
    // `remove` returned null for an absent entry.
    pathLock.release(path)
  }
}
