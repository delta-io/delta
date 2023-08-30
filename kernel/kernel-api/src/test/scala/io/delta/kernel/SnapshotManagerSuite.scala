/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel

import java.util.{Arrays, Collections, Optional}

import io.delta.kernel.internal.snapshot.SnapshotManager
import org.scalatest.funsuite.AnyFunSuite

class SnapshotManagerSuite extends AnyFunSuite {

  test("verifyDeltaVersions") {
    // empty array
    SnapshotManager.verifyDeltaVersions(
      Collections.emptyList(),
      Optional.empty(),
      Optional.empty())
    // contiguous versions
    SnapshotManager.verifyDeltaVersions(
      Arrays.asList(1, 2, 3),
      Optional.empty(),
      Optional.empty())
    // contiguous versions with correct `expectedStartVersion` and `expectedStartVersion`
    SnapshotManager.verifyDeltaVersions(
      Arrays.asList(1, 2, 3),
      Optional.empty(),
      Optional.of(3))
    SnapshotManager.verifyDeltaVersions(
      Arrays.asList(1, 2, 3),
      Optional.of(1),
      Optional.empty())
    SnapshotManager.verifyDeltaVersions(
      Arrays.asList(1, 2, 3),
      Optional.of(1),
      Optional.of(3))
    // `expectedStartVersion` or `expectedEndVersion` doesn't match
    intercept[IllegalArgumentException] {
      SnapshotManager.verifyDeltaVersions(
        Arrays.asList(1, 2),
        Optional.of(0),
        Optional.empty())
    }
    intercept[IllegalArgumentException] {
      SnapshotManager.verifyDeltaVersions(
        Arrays.asList(1, 2),
        Optional.empty(),
        Optional.of(3))
    }
    intercept[IllegalArgumentException] {
      SnapshotManager.verifyDeltaVersions(
        Collections.emptyList(),
        Optional.of(0),
        Optional.empty())
    }
    intercept[IllegalArgumentException] {
      SnapshotManager.verifyDeltaVersions(
        Collections.emptyList(),
        Optional.empty(),
        Optional.of(3))
    }
    // non contiguous versions
    intercept[IllegalStateException] {
      SnapshotManager.verifyDeltaVersions(
        Arrays.asList(1, 3),
        Optional.empty(),
        Optional.empty())
    }
    // duplicates in versions
    intercept[IllegalStateException] {
      SnapshotManager.verifyDeltaVersions(
        Arrays.asList(1, 2, 2, 3),
        Optional.empty(),
        Optional.empty())
    }
    // unsorted versions
    intercept[IllegalStateException] {
      SnapshotManager.verifyDeltaVersions(
        Arrays.asList(3, 2, 1),
        Optional.empty(),
        Optional.empty())
    }
  }
}
