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
package io.delta.kernel.internal

import io.delta.kernel.internal.fs.Path
import io.delta.kernel.test.MockSnapshotUtils

import org.scalatest.funsuite.AnyFunSuite

class SnapshotImplSuite extends AnyFunSuite with MockSnapshotUtils {

  test("equals and hashCode - same path and version are equal") {
    val path = new Path("/table/path")
    val snap1 = getMockSnapshot(path, latestVersion = 5)
    val snap2 = getMockSnapshot(path, latestVersion = 5)

    assert(!(snap1 eq snap2), "Expected different object references")
    assert(snap1 == snap2, "Snapshots with same path and version should be equal")
    assert(snap1.hashCode == snap2.hashCode,
      "Equal snapshots should have the same hashCode")
  }

  test("equals - different versions are not equal") {
    val path = new Path("/table/path")
    val snap1 = getMockSnapshot(path, latestVersion = 5)
    val snap2 = getMockSnapshot(path, latestVersion = 6)

    assert(snap1 != snap2, "Snapshots at different versions should not be equal")
  }

  test("equals - different paths are not equal") {
    val snap1 = getMockSnapshot(new Path("/table/a"), latestVersion = 5)
    val snap2 = getMockSnapshot(new Path("/table/b"), latestVersion = 5)

    assert(snap1 != snap2, "Snapshots for different tables should not be equal")
  }

  test("equals - reflexive, symmetric, and transitive") {
    val path = new Path("/table/path")
    val a = getMockSnapshot(path, latestVersion = 3)
    val b = getMockSnapshot(path, latestVersion = 3)
    val c = getMockSnapshot(path, latestVersion = 3)

    // reflexive
    assert(a == a)
    // symmetric
    assert(a == b)
    assert(b == a)
    // transitive
    assert(a == b)
    assert(b == c)
    assert(a == c)
  }

  test("equals - not equal to null or other types") {
    val snap = getMockSnapshot(new Path("/table/path"), latestVersion = 1)
    assert(snap != null)
    assert(!snap.equals("not a snapshot"))
  }

  test("hashCode - consistent across calls") {
    val snap = getMockSnapshot(new Path("/table/path"), latestVersion = 7)
    val h1 = snap.hashCode
    val h2 = snap.hashCode
    assert(h1 == h2, "hashCode should be consistent")
  }
}
