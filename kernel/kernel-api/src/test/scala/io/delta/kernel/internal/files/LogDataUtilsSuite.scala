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

package io.delta.kernel.internal.files

import scala.collection.JavaConverters._

import io.delta.kernel.test.{MockFileSystemClientUtils, VectorTestUtils}

import org.scalatest.funsuite.AnyFunSuite

class LogDataUtilsSuite extends AnyFunSuite with MockFileSystemClientUtils with VectorTestUtils {

  private val emptyInlineData = emptyColumnarBatch

  //////////////////////////////////////////////////////
  // validateLogDataContainsOnlyRatifiedStagedCommits //
  //////////////////////////////////////////////////////

  test("validateLogDataContainsOnlyRatifiedStagedCommits: empty list passes") {
    // Should not throw any exception
    LogDataUtils.validateLogDataContainsOnlyRatifiedStagedCommits(Seq.empty.asJava)
  }

  test("validateLogDataContainsOnlyRatifiedStagedCommits: valid list passes") {
    val logDatas = Seq(
      ParsedCatalogCommitData.forFileStatus(stagedCommitFile(1)),
      ParsedCatalogCommitData.forFileStatus(stagedCommitFile(2)))
    // Should not throw any exception
    LogDataUtils.validateLogDataContainsOnlyRatifiedStagedCommits(logDatas.asJava)
  }

  test("validateLogDataContainsOnlyRatifiedStagedCommits: inline delta fails") {
    intercept[IllegalArgumentException] {
      LogDataUtils.validateLogDataContainsOnlyRatifiedStagedCommits(
        Seq(ParsedCatalogCommitData.forInlineData(3, emptyInlineData)).asJava)
    }
  }

  test("validateLogDataContainsOnlyRatifiedStagedCommits: published delta fails") {
    intercept[IllegalArgumentException] {
      LogDataUtils.validateLogDataContainsOnlyRatifiedStagedCommits(
        Seq(ParsedPublishedDeltaData.forFileStatus(deltaFileStatus(1))).asJava)
    }
  }

  test("validateLogDataContainsOnlyRatifiedStagedCommits: checkpoint data fails") {
    intercept[IllegalArgumentException] {
      LogDataUtils.validateLogDataContainsOnlyRatifiedStagedCommits(
        Seq(ParsedClassicCheckpointData.forFileStatus(classicCheckpointFileStatus(0))).asJava)
    }
  }

  ///////////////////////////////////////
  // validateLogDataIsSortedContiguous //
  ///////////////////////////////////////

  test("validateLogDataIsSortedContiguous: empty list should pass") {
    // Should not throw any exception
    LogDataUtils.validateLogDataIsSortedContiguous(Seq.empty.asJava)
  }

  test("validateLogDataIsSortedContiguous: single element should pass") {
    val singleElement = Seq(ParsedDeltaData.forFileStatus(deltaFileStatus(1)))

    // Should not throw any exception
    LogDataUtils.validateLogDataIsSortedContiguous(singleElement.asJava)
  }

  test("validateLogDataIsSortedContiguous: contiguous versions should pass") {
    val contiguousData = Seq(
      ParsedDeltaData.forFileStatus(deltaFileStatus(1)),
      ParsedDeltaData.forFileStatus(deltaFileStatus(2)),
      ParsedDeltaData.forFileStatus(deltaFileStatus(3)))

    // Should not throw any exception
    LogDataUtils.validateLogDataIsSortedContiguous(contiguousData.asJava)
  }

  test("validateLogDataIsSortedContiguous: non-contiguous versions should fail") {
    val nonContiguousData = Seq(
      ParsedDeltaData.forFileStatus(deltaFileStatus(1)),
      ParsedDeltaData.forFileStatus(deltaFileStatus(3)) // Missing version 2
    )

    intercept[IllegalArgumentException] {
      LogDataUtils.validateLogDataIsSortedContiguous(nonContiguousData.asJava)
    }
  }

  test("validateLogDataIsSortedContiguous: unsorted versions should fail") {
    val unsortedData = Seq(
      ParsedDeltaData.forFileStatus(deltaFileStatus(2)),
      ParsedDeltaData.forFileStatus(deltaFileStatus(1)))

    intercept[IllegalArgumentException] {
      LogDataUtils.validateLogDataIsSortedContiguous(unsortedData.asJava)
    }
  }

  test("validateLogDataIsSortedContiguous: duplicate versions should fail") {
    val duplicateData = Seq(
      ParsedDeltaData.forFileStatus(deltaFileStatus(1)),
      ParsedDeltaData.forFileStatus(deltaFileStatus(1)))

    intercept[IllegalArgumentException] {
      LogDataUtils.validateLogDataIsSortedContiguous(duplicateData.asJava)
    }
  }

  test("validateLogDataIsSortedContiguous: mixed log data types should work") {
    val mixedData = Seq(
      ParsedDeltaData.forFileStatus(deltaFileStatus(1)),
      ParsedLogData.forFileStatus(checksumFileStatus(2)),
      ParsedLogData.forFileStatus(classicCheckpointFileStatus(3)))

    // Should not throw any exception
    LogDataUtils.validateLogDataIsSortedContiguous(mixedData.asJava)
  }

  //////////////////////////////////////////////////////////
  // combinePublishedAndRatifiedDeltasWithCatalogPriority //
  //////////////////////////////////////////////////////////

  test("combinePublishedAndRatifiedDeltasWithCatalogPriority: empty published, empty ratified") {
    val result = LogDataUtils.combinePublishedAndRatifiedDeltasWithCatalogPriority(
      Seq.empty.asJava,
      Seq.empty.asJava)
    assert(result.isEmpty)
  }

  test(
    "combinePublishedAndRatifiedDeltasWithCatalogPriority: empty published, non-empty ratified") {
    val ratifiedDeltas = Seq(
      ParsedDeltaData.forFileStatus(stagedCommitFile(1)),
      ParsedDeltaData.forFileStatus(stagedCommitFile(2)))

    val result = LogDataUtils.combinePublishedAndRatifiedDeltasWithCatalogPriority(
      Seq.empty.asJava,
      ratifiedDeltas.asJava)

    assert(result.asScala === ratifiedDeltas)
  }

  test(
    "combinePublishedAndRatifiedDeltasWithCatalogPriority: non-empty published, empty ratified") {
    val publishedDeltas = Seq(
      ParsedDeltaData.forFileStatus(deltaFileStatus(1)),
      ParsedDeltaData.forFileStatus(deltaFileStatus(2)))

    val result = LogDataUtils.combinePublishedAndRatifiedDeltasWithCatalogPriority(
      publishedDeltas.asJava,
      Seq.empty.asJava)

    assert(result.asScala === publishedDeltas)
  }

  test("combinePublishedAndRatifiedDeltasWithCatalogPriority: non-overlapping ranges") {
    val publishedDeltas = Seq(
      ParsedDeltaData.forFileStatus(deltaFileStatus(1)),
      ParsedDeltaData.forFileStatus(deltaFileStatus(2)))
    val ratifiedDeltas = Seq(
      ParsedDeltaData.forFileStatus(stagedCommitFile(3)),
      ParsedDeltaData.forFileStatus(stagedCommitFile(4)))

    val result = LogDataUtils.combinePublishedAndRatifiedDeltasWithCatalogPriority(
      publishedDeltas.asJava,
      ratifiedDeltas.asJava)

    assert(result.asScala === publishedDeltas ++ ratifiedDeltas)
  }

  test(
    "combinePublishedAndRatifiedDeltasWithCatalogPriority: " +
      "overlapping ranges - ratified priority") {
    val publishedDeltas = Seq(
      ParsedDeltaData.forFileStatus(deltaFileStatus(1)),
      ParsedDeltaData.forFileStatus(deltaFileStatus(2)))
    val ratifiedDeltas = Seq(
      ParsedDeltaData.forFileStatus(stagedCommitFile(2)),
      ParsedDeltaData.forFileStatus(stagedCommitFile(3)))

    val result = LogDataUtils.combinePublishedAndRatifiedDeltasWithCatalogPriority(
      publishedDeltas.asJava,
      ratifiedDeltas.asJava)

    val expected = Seq(publishedDeltas.head) ++ ratifiedDeltas
    assert(result.asScala === expected)
  }

  test("combinePublishedAndRatifiedDeltasWithCatalogPriority: ratified in middle of published") {
    val publishedDeltas = Seq(
      ParsedDeltaData.forFileStatus(deltaFileStatus(1)),
      ParsedDeltaData.forFileStatus(deltaFileStatus(2)),
      ParsedDeltaData.forFileStatus(deltaFileStatus(3)),
      ParsedDeltaData.forFileStatus(deltaFileStatus(4)))
    val ratifiedDeltas = Seq(
      ParsedDeltaData.forFileStatus(stagedCommitFile(2)),
      ParsedDeltaData.forFileStatus(stagedCommitFile(3)))

    val result = LogDataUtils.combinePublishedAndRatifiedDeltasWithCatalogPriority(
      publishedDeltas.asJava,
      ratifiedDeltas.asJava)

    val expected = Seq(publishedDeltas.head) ++ ratifiedDeltas ++ Seq(publishedDeltas(3))

    assert(result.asScala === expected)
  }

  test("combinePublishedAndRatifiedDeltasWithCatalogPriority: single ratified version") {
    val publishedDeltas = Seq(
      ParsedDeltaData.forFileStatus(deltaFileStatus(1)),
      ParsedDeltaData.forFileStatus(deltaFileStatus(2)),
      ParsedDeltaData.forFileStatus(deltaFileStatus(3)))
    val ratifiedDeltas = Seq(
      ParsedDeltaData.forFileStatus(stagedCommitFile(3)))

    val result = LogDataUtils.combinePublishedAndRatifiedDeltasWithCatalogPriority(
      publishedDeltas.asJava,
      ratifiedDeltas.asJava)

    val expected = Seq(publishedDeltas(0), publishedDeltas(1)) ++ ratifiedDeltas

    assert(result.asScala === expected)
  }
}
