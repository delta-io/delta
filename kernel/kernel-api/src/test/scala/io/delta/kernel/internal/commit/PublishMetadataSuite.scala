/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.kernel.internal.commit

import scala.collection.JavaConverters._

import io.delta.kernel.commit.PublishMetadata
import io.delta.kernel.test.TestFixtures

import org.scalatest.funsuite.AnyFunSuite

class PublishMetadataSuite extends AnyFunSuite with TestFixtures {

  private val logPath = "/fake/_delta_log"

  ////////////////////
  // Negative Tests //
  ////////////////////

  test("constructor validates null logPath") {
    val commits = List(createStagedCatalogCommit(1)).asJava
    intercept[NullPointerException] {
      new PublishMetadata(1, null, commits)
    }
  }

  test("constructor validates null ascendingCatalogCommits") {
    intercept[NullPointerException] {
      new PublishMetadata(1, logPath, null)
    }
  }

  test("constructor validates non-empty commits") {
    val ex = intercept[IllegalArgumentException] {
      new PublishMetadata(1, logPath, List.empty.asJava)
    }
    assert(ex.getMessage.contains("ascendingCatalogCommits must be non-empty"))
  }

  test("constructor validates contiguous commits - gap in sequence") {
    val commits = List(
      createStagedCatalogCommit(1),
      createStagedCatalogCommit(2),
      createStagedCatalogCommit(4) // Gap: missing version 3
    ).asJava

    val ex = intercept[IllegalArgumentException] {
      new PublishMetadata(4, logPath, commits)
    }
    assert(ex.getMessage.contains("must be sorted and contiguous"))
  }

  test("constructor validates sorted commits - out of order") {
    val commits = List(
      createStagedCatalogCommit(2),
      createStagedCatalogCommit(1), // Out of order
      createStagedCatalogCommit(3)).asJava

    val ex = intercept[IllegalArgumentException] {
      new PublishMetadata(3, logPath, commits)
    }
    assert(ex.getMessage.contains("must be sorted and contiguous"))
  }

  test("constructor validates last commit matches snapshot version") {
    val commits = List(
      createStagedCatalogCommit(1),
      createStagedCatalogCommit(2),
      createStagedCatalogCommit(3)).asJava

    val ex = intercept[IllegalArgumentException] {
      new PublishMetadata(5, logPath, commits) // Snapshot is 5, but last commit is 3
    }
    assert(ex.getMessage.contains("Last catalog commit version 3 must equal snapshot version 5"))
  }

  ////////////////////
  // Positive Tests //
  ////////////////////

  test("valid construction with single commit") {
    val commits = List(createStagedCatalogCommit(5)).asJava
    val publishMetadata = new PublishMetadata(5, logPath, commits)

    assert(publishMetadata.getSnapshotVersion == 5)
    assert(publishMetadata.getLogPath == logPath)
    assert(publishMetadata.getAscendingCatalogCommits == commits)
  }

  test("valid construction with multiple contiguous commits") {
    val commits = List(
      createStagedCatalogCommit(3),
      createStagedCatalogCommit(4),
      createStagedCatalogCommit(5)).asJava
    val publishMetadata = new PublishMetadata(5, logPath, commits)

    assert(publishMetadata.getSnapshotVersion == 5)
    assert(publishMetadata.getAscendingCatalogCommits.size() == 3)
  }
}
