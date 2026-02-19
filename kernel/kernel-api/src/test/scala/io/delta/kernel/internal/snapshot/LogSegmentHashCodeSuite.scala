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

package io.delta.kernel.internal.snapshot

import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

import io.delta.kernel.internal.fs.Path
import io.delta.kernel.test.MockFileSystemClientUtils
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite for LogSegment.hashCode() to verify that the hash calculation
 * is based on logical versions rather than physical file names, enabling
 * pagination to work correctly when staged commits are renamed to published commits.
 *
 * Addresses issue #4927: Pagination should continue working when a file changes
 * from 9.uuid.json (staged) to 9.json (published).
 */
class LogSegmentHashCodeSuite extends AnyFunSuite with MockFileSystemClientUtils {

  test("hashCode should be equal for staged and published commits of same version") {
    // Create LogSegment with staged commit (9.uuid.json)
    val stagedCommit = stagedCommitFile(9)
    val logSegmentStaged = new LogSegment(
      logPath,
      9L,
      Collections.singletonList(stagedCommit),
      Collections.emptyList(), // compactions
      Collections.emptyList(), // checkpoints
      stagedCommit,
      Optional.empty(),
      Optional.empty())

    // Create LogSegment with published commit (9.json)
    val publishedCommit = deltaFileStatus(9)
    val logSegmentPublished = new LogSegment(
      logPath,
      9L,
      Collections.singletonList(publishedCommit),
      Collections.emptyList(),
      Collections.emptyList(),
      publishedCommit,
      Optional.empty(),
      Optional.empty())

    assert(
      logSegmentStaged.hashCode() == logSegmentPublished.hashCode(),
      "Hash codes should be equal for staged and published commits of same version")
  }

  test("hashCode should differ for different versions") {
    val commit9 = deltaFileStatus(9)
    val commit10 = deltaFileStatus(10)

    val logSegment9 = new LogSegment(
      logPath,
      9L,
      Collections.singletonList(commit9),
      Collections.emptyList(),
      Collections.emptyList(),
      commit9,
      Optional.empty(),
      Optional.empty())

    val logSegment10 = new LogSegment(
      logPath,
      10L,
      Collections.singletonList(commit10),
      Collections.emptyList(),
      Collections.emptyList(),
      commit10,
      Optional.empty(),
      Optional.empty())

    assert(
      logSegment9.hashCode() != logSegment10.hashCode(),
      "Hash codes should differ for different versions")
  }

  test("hashCode with multiple deltas - staged vs published") {
    val deltasStaged = Seq(
      deltaFileStatus(7),
      stagedCommitFile(8),
      deltaFileStatus(9)).asJava

    val deltasPublished = Seq(
      deltaFileStatus(7),
      deltaFileStatus(8),
      deltaFileStatus(9)).asJava

    val logSegment1 = new LogSegment(
      logPath,
      9L,
      deltasStaged,
      Collections.emptyList(),
      Collections.emptyList(),
      deltaFileStatus(9),
      Optional.empty(),
      Optional.empty())

    val logSegment2 = new LogSegment(
      logPath,
      9L,
      deltasPublished,
      Collections.emptyList(),
      Collections.emptyList(),
      deltaFileStatus(9),
      Optional.empty(),
      Optional.empty())

    assert(
      logSegment1.hashCode() == logSegment2.hashCode(),
      "Hash codes should be equal when versions are same regardless of staged/published state")
  }

  test("hashCode with different UUID for same version should be equal") {
    val stagedCommit1 = FileStatus.of(
      s"${logPath.toString}/_staged_commits/00000000000000000009.uuid-1111.json",
      1000L,
      1234567890L)
    val stagedCommit2 = FileStatus.of(
      s"${logPath.toString}/_staged_commits/00000000000000000009.uuid-2222.json",
      1000L,
      1234567890L)

    val logSegment1 = new LogSegment(
      logPath,
      9L,
      Collections.singletonList(stagedCommit1),
      Collections.emptyList(),
      Collections.emptyList(),
      stagedCommit1,
      Optional.empty(),
      Optional.empty())

    val logSegment2 = new LogSegment(
      logPath,
      9L,
      Collections.singletonList(stagedCommit2),
      Collections.emptyList(),
      Collections.emptyList(),
      stagedCommit2,
      Optional.empty(),
      Optional.empty())

    assert(
      logSegment1.hashCode() == logSegment2.hashCode(),
      "Hash codes should be equal for different staged commits of same version")
  }

  test("hashCode with checkpoints should be based on version") {
    val checkpoint10 = singularCheckpointFileStatuses(Seq(10)).head
    val deltas = deltaFileStatuses(Seq(11, 12)).toList.asJava

    val logSegment1 = new LogSegment(
      logPath,
      12L,
      deltas,
      Collections.emptyList(),
      Collections.singletonList(checkpoint10),
      deltaFileStatus(12),
      Optional.empty(),
      Optional.empty())

    val logSegment2 = new LogSegment(
      logPath,
      12L,
      deltas,
      Collections.emptyList(),
      Collections.singletonList(checkpoint10),
      deltaFileStatus(12),
      Optional.empty(),
      Optional.empty())

    assert(
      logSegment1.hashCode() == logSegment2.hashCode(),
      "Hash codes should be equal for same checkpoint and delta versions")
  }

  test("hashCode with compactions should be based on start and end versions") {
    val deltas = deltaFileStatuses(Seq(7, 8, 9, 10)).toList.asJava
    val compaction = compactedFileStatuses(Seq((8, 9))).head

    val logSegment1 = new LogSegment(
      logPath,
      10L,
      deltas,
      Collections.singletonList(compaction),
      Collections.emptyList(),
      deltaFileStatus(10),
      Optional.empty(),
      Optional.empty())

    val logSegment2 = new LogSegment(
      logPath,
      10L,
      deltas,
      Collections.singletonList(compaction),
      Collections.emptyList(),
      deltaFileStatus(10),
      Optional.empty(),
      Optional.empty())

    assert(
      logSegment1.hashCode() == logSegment2.hashCode(),
      "Hash codes should be equal for same compaction versions")
  }

  test("hashCode should differ when delta versions are different") {
    val deltas1 = deltaFileStatuses(Seq(7, 8, 9)).toList.asJava
    val deltas2 = deltaFileStatuses(Seq(8, 9, 10)).toList.asJava

    val logSegment1 = new LogSegment(
      logPath,
      9L,
      deltas1,
      Collections.emptyList(),
      Collections.emptyList(),
      deltaFileStatus(9),
      Optional.empty(),
      Optional.empty())

    val logSegment2 = new LogSegment(
      logPath,
      10L,
      deltas2,
      Collections.emptyList(),
      Collections.emptyList(),
      deltaFileStatus(10),
      Optional.empty(),
      Optional.empty())

    assert(
      logSegment1.hashCode() != logSegment2.hashCode(),
      "Hash codes should differ when delta versions are different")
  }

  test("hashCode with all staged commits should equal all published commits") {
    val allStaged = Seq(
      stagedCommitFile(7),
      stagedCommitFile(8),
      stagedCommitFile(9)).asJava

    val allPublished = deltaFileStatuses(Seq(7, 8, 9)).toList.asJava

    val logSegmentStaged = new LogSegment(
      logPath,
      9L,
      allStaged,
      Collections.emptyList(),
      Collections.emptyList(),
      stagedCommitFile(9),
      Optional.empty(),
      Optional.empty())

    val logSegmentPublished = new LogSegment(
      logPath,
      9L,
      allPublished,
      Collections.emptyList(),
      Collections.emptyList(),
      deltaFileStatus(9),
      Optional.empty(),
      Optional.empty())

    assert(
      logSegmentStaged.hashCode() == logSegmentPublished.hashCode(),
      "Hash codes should be equal when all commits are same versions (staged vs published)")
  }

  test("hashCode should be stable across multiple invocations") {
    val deltas = deltaFileStatuses(Seq(7, 8, 9)).toList.asJava
    val logSegment = new LogSegment(
      logPath,
      9L,
      deltas,
      Collections.emptyList(),
      Collections.emptyList(),
      deltaFileStatus(9),
      Optional.empty(),
      Optional.empty())

    val hash1 = logSegment.hashCode()
    val hash2 = logSegment.hashCode()
    val hash3 = logSegment.hashCode()

    assert(
      hash1 == hash2 && hash2 == hash3,
      "hashCode should return same value across multiple invocations")
  }
}
