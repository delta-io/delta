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

package io.delta.unity

import java.net.URI
import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.storage.commit.CommitFailedException
import io.delta.storage.commit.uccommitcoordinator.InvalidTargetTableException

import org.scalatest.funsuite.AnyFunSuite

/** Unit tests for [[InMemoryUCClient]]. */
class InMemoryUCClientSuite extends AnyFunSuite with UCCatalogManagedTestUtils {

  private val fakeURI = new URI("s3://bucket/table")

  private def getInMemoryUCClientWithCommitsForTableId(
      tableId: String,
      versions: Seq[Long]): InMemoryUCClient = {
    val client = new InMemoryUCClient("ucMetastoreId")
    val tableData = new InMemoryUCClient.TableData
    versions.foreach { v => tableData.appendCommit(createCommit(v)) }
    client.tables.put(tableId, tableData)
    client
  }

  test("TableData::appendCommit throws if commit version is not maxRatifiedVersion + 1") {
    val tableData = new InMemoryUCClient.TableData
    val commit = createCommit(1L)

    val exMsg = intercept[CommitFailedException] {
      tableData.appendCommit(commit)
    }.getMessage

    assert(exMsg.contains("Expected commit version 0 but got 1"))
  }

  test("TableData::appendCommit appends the commit and updates the maxRatifiedVersion") {
    val tableData = new InMemoryUCClient.TableData
    tableData.appendCommit(createCommit(0L))

    assert(tableData.getMaxRatifiedVersion == 0L)
    assert(tableData.getCommits.size == 1)
    assert(tableData.getCommits.head.getVersion == 0L)

    tableData.appendCommit(createCommit(1L))
    assert(tableData.getMaxRatifiedVersion == 1L)
    assert(tableData.getCommits.size == 2)
    assert(tableData.getCommits.last.getVersion == 1L)
  }

  test("getCommits throws InvalidTargetTableException for non-existent table") {
    val client = new InMemoryUCClient("ucMetastoreId")
    val tableId = "non-existent-table"

    val exception = intercept[InvalidTargetTableException] {
      client.getCommits(tableId, new URI("s3://bucket/table"), Optional.empty(), Optional.empty())
    }
    assert(exception.getMessage.contains(s"Table not found: $tableId"))
  }

  test("getCommits returns all commits if no startVersion or endVersion filter") {
    val client = getInMemoryUCClientWithCommitsForTableId("tableId", 0L to 5L)
    val response = client.getCommits("tableId", fakeURI, Optional.empty(), Optional.empty())
    assert(response.getCommits.asScala.map(_.getVersion) sameElements (0L to 5L).toList)
  }

  test("getCommits filters by startVersion") {
    val client = getInMemoryUCClientWithCommitsForTableId("tableId", 0L to 5L)
    val response = client.getCommits("tableId", fakeURI, Optional.of(2L), Optional.empty())
    assert(response.getCommits.asScala.map(_.getVersion) sameElements (2L to 5L).toList)
  }

  test("getCommits filters by endVersion") {
    val client = getInMemoryUCClientWithCommitsForTableId("tableId", 0L to 5L)
    val response = client.getCommits("tableId", fakeURI, Optional.empty(), Optional.of(3L))
    assert(response.getCommits.asScala.map(_.getVersion) sameElements (0L to 3L).toList)
  }

  test("getCommits filters by startVersion and endVersion") {
    val client = getInMemoryUCClientWithCommitsForTableId("tableId", 0L to 5L)
    val response = client.getCommits("tableId", fakeURI, Optional.of(2L), Optional.of(4L))
    assert(response.getCommits.asScala.map(_.getVersion) sameElements (2L to 4L).toList)
  }

}
