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

package io.delta.kernel.unitycatalog

import java.lang.{Long => JLong}
import java.net.URI
import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.storage.commit.CommitFailedException
import io.delta.storage.commit.uccommitcoordinator.InvalidTargetTableException

import org.scalatest.funsuite.AnyFunSuite

/** Unit tests for [[InMemoryUCClient]]. */
class InMemoryUCClientSuite extends AnyFunSuite with UCCatalogManagedTestUtils {

  private def testGetCommitsFiltering(
      allVersions: Seq[Long],
      startVersionOpt: Optional[JLong],
      endVersionOpt: Optional[JLong],
      expectedVersions: Seq[Long]): Unit = {
    val client = getInMemoryUCClientWithCommitsForTableId("tableId", allVersions)
    val response = client.getCommits("tableId", fakeURI, startVersionOpt, endVersionOpt)
    val actualVersions = response.getCommits.asScala.map(_.getVersion)

    assert(actualVersions == expectedVersions)
  }

  // TODO: [delta-io/delta#5118] If UC changes CREATE semantics, update logic here.
  test("TableData::appendCommit throws on commit v0 (since CREATE does not go through UC)") {
    val tableData = new InMemoryUCClient.TableData

    val exMsg = intercept[CommitFailedException] {
      tableData.appendCommit(createCommit(0L))
    }.getMessage

    assert(exMsg.contains("Expected commit version 1 but got 0"))
  }

  // TODO: [delta-io/delta#5118] If UC changes CREATE semantics, update logic here.
  test("TableData::appendCommit handles commit version 1 (since CREATE does not go through UC)") {
    val tableData = new InMemoryUCClient.TableData
    assert(tableData.getMaxRatifiedVersion == -1L)

    tableData.appendCommit(createCommit(1L))

    assert(tableData.getMaxRatifiedVersion == 1L)
    assert(tableData.getCommits.size == 1)
    assert(tableData.getCommits.head.getVersion == 1L)
  }

  test("TableData::appendCommit throws if commit version is not maxRatifiedVersion + 1 " +
    "(excluding v1 edge case)") {
    val tableData = new InMemoryUCClient.TableData
    tableData.appendCommit(createCommit(1L))

    val exMsg = intercept[CommitFailedException] {
      tableData.appendCommit(createCommit(99L))
    }.getMessage

    assert(exMsg.contains("Expected commit version 2 but got 99"))
  }

  test("TableData::appendCommit appends the commit and updates the maxRatifiedVersion") {
    val tableData = new InMemoryUCClient.TableData
    tableData.appendCommit(createCommit(1L))

    assert(tableData.getMaxRatifiedVersion == 1L)
    assert(tableData.getCommits.size == 1)
    assert(tableData.getCommits.head.getVersion == 1L)

    tableData.appendCommit(createCommit(2L))
    assert(tableData.getMaxRatifiedVersion == 2L)
    assert(tableData.getCommits.size == 2)
    assert(tableData.getCommits.last.getVersion == 2L)
  }

  test("getCommits throws InvalidTargetTableException for non-existent table") {
    val client = new InMemoryUCClient("ucMetastoreId")
    val exception = intercept[InvalidTargetTableException] {
      client.getCommits("abcd", new URI("s3://bucket/table"), Optional.empty(), Optional.empty())
    }
    assert(exception.getMessage.contains(s"Table not found: abcd"))
  }

  test("getCommits returns all commits if no startVersion or endVersion filter") {
    testGetCommitsFiltering(
      allVersions = 1L to 5L,
      startVersionOpt = Optional.empty(),
      endVersionOpt = Optional.empty(),
      expectedVersions = 1L to 5L)
  }

  test("getCommits filters by startVersion") {
    testGetCommitsFiltering(
      allVersions = 1L to 5L,
      startVersionOpt = Optional.of(2L),
      endVersionOpt = Optional.empty(),
      expectedVersions = 2L to 5L)
  }

  test("getCommits filters by endVersion") {
    testGetCommitsFiltering(
      allVersions = 1L to 5L,
      startVersionOpt = Optional.empty(),
      endVersionOpt = Optional.of(3L),
      expectedVersions = 1L to 3L)
  }

  test("getCommits filters by startVersion and endVersion") {
    testGetCommitsFiltering(
      allVersions = 1L to 5L,
      startVersionOpt = Optional.of(2L),
      endVersionOpt = Optional.of(4L),
      expectedVersions = 2L to 4L)
  }

}
