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
package io.delta.kernel.spark.snapshot.unitycatalog

import java.io.IOException
import java.net.URI
import java.util.Optional

import scala.jdk.CollectionConverters._

import io.delta.kernel.internal.util.FileNames
import io.delta.storage.commit.{Commit, GetCommitsResponse}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.storage.commit.uccommitcoordinator.{CommitLimitReachedException, UCClient}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.scalatest.funsuite.AnyFunSuite

class UnityCatalogAdapterSuite extends AnyFunSuite {

  private val tableId = "testTableId"
  private val tablePath = "/tmp/test-table"
  private val tableUri = new URI(tablePath)

  private def hadoopStatus(version: Long): FileStatus = {
    val stagedPath = new Path(FileNames.stagedCommitFile(s"$tablePath/_delta_log", version))
    new FileStatus(/* length = */ 10L, /* isDir = */ false, /* blockReplication = */ 1, /* blockSize = */ 1,
      /* modificationTime = */ version, stagedPath)
  }

  private def commit(version: Long): Commit = new Commit(version, hadoopStatus(version), version)

  private class StubUCClient(
      response: => GetCommitsResponse,
      throwOnGet: Option[Throwable] = None) extends UCClient {
    override def getMetastoreId(): String = "meta"

    override def commit(
        tableId: String,
        tableUri: URI,
        commit: Optional[Commit],
        lastKnownBackfilledVersion: Optional[java.lang.Long],
        disown: Boolean,
        newMetadata: Optional[AbstractMetadata],
        newProtocol: Optional[AbstractProtocol]): Unit = {
      throw new UnsupportedOperationException("not used in tests")
    }

    override def getCommits(
        tableId: String,
        tableUri: URI,
        startVersion: Optional[java.lang.Long],
        endVersion: Optional[java.lang.Long]): GetCommitsResponse = {
      throwOnGet.foreach {
        case io: IOException => throw io
        case other: Throwable => throw other
      }
      response
    }

    override def close(): Unit = {}
  }

  test("getCommits returns response from UCClient") {
    val commits = List(2L, 0L, 1L).map(commit)
    val adapter = new UnityCatalogAdapter(tableId, tablePath, new StubUCClient(
      new GetCommitsResponse(commits.asJava, /* latest */ 2L)))

    val response = adapter.getCommits(0, Optional.empty())
    val versions = response.getCommits.asScala.map(_.getVersion)
    assert(versions.toSet == Set(0L, 1L, 2L))
    assert(response.getLatestTableVersion == 2L)
  }

  test("getLatestRatifiedVersion returns latest version from response") {
    val commits = List(0L, 1L, 2L).map(commit)
    val adapter = new UnityCatalogAdapter(tableId, tablePath, new StubUCClient(
      new GetCommitsResponse(commits.asJava, /* latest */ 2L)))

    assert(adapter.getLatestRatifiedVersion() == 2L)
  }

  test("getLatestRatifiedVersion maps -1 to 0") {
    val adapter = new UnityCatalogAdapter(tableId, tablePath, new StubUCClient(
      new GetCommitsResponse(List.empty[Commit].asJava, /* latest */ -1L)))

    assert(adapter.getLatestRatifiedVersion() == 0L)
  }

  test("getCommits wraps IO failures") {
    val adapter = new UnityCatalogAdapter(tableId, tablePath, new StubUCClient(
      response = null,
      throwOnGet = Some(new IOException("boom"))))

    assertThrows[java.io.UncheckedIOException] {
      adapter.getCommits(0, Optional.empty())
    }
  }

  test("getCommits wraps UC coordinator failures") {
    val adapter = new UnityCatalogAdapter(tableId, tablePath, new StubUCClient(
      response = null,
      throwOnGet = Some(new CommitLimitReachedException("boom"))))

    assertThrows[RuntimeException] {
      adapter.getCommits(0, Optional.empty())
    }
  }

  test("getCommits passes endVersion to UCClient") {
    var capturedEndVersion: Optional[java.lang.Long] = null
    val commits = List(0L, 1L, 2L).map(commit)

    val capturingClient = new UCClient {
      override def getMetastoreId(): String = "meta"
      override def commit(
          tableId: String,
          tableUri: URI,
          commit: Optional[Commit],
          lastKnownBackfilledVersion: Optional[java.lang.Long],
          disown: Boolean,
          newMetadata: Optional[AbstractMetadata],
          newProtocol: Optional[AbstractProtocol]): Unit = {
        throw new UnsupportedOperationException("not used")
      }
      override def getCommits(
          tableId: String,
          tableUri: URI,
          startVersion: Optional[java.lang.Long],
          endVersion: Optional[java.lang.Long]): GetCommitsResponse = {
        capturedEndVersion = endVersion
        new GetCommitsResponse(commits.asJava, /* latest */ 2L)
      }
      override def close(): Unit = {}
    }

    val adapter = new UnityCatalogAdapter(tableId, tablePath, capturingClient)

    // Test with specific endVersion
    adapter.getCommits(0, Optional.of(java.lang.Long.valueOf(1L)))
    assert(capturedEndVersion.isPresent)
    assert(capturedEndVersion.get() == 1L)

    // Test with empty endVersion
    adapter.getCommits(0, Optional.empty())
    assert(!capturedEndVersion.isPresent)
  }

  test("getCommits filters commits up to endVersion") {
    val allCommits = List(0L, 1L, 2L, 3L).map(commit)

    // Client returns filtered commits when endVersion specified
    def clientWithEndVersionFilter(endOpt: Optional[java.lang.Long]): UCClient = new UCClient {
      override def getMetastoreId(): String = "meta"
      override def commit(
          tableId: String,
          tableUri: URI,
          commit: Optional[Commit],
          lastKnownBackfilledVersion: Optional[java.lang.Long],
          disown: Boolean,
          newMetadata: Optional[AbstractMetadata],
          newProtocol: Optional[AbstractProtocol]): Unit = {
        throw new UnsupportedOperationException("not used")
      }
      override def getCommits(
          tableId: String,
          tableUri: URI,
          startVersion: Optional[java.lang.Long],
          endVersion: Optional[java.lang.Long]): GetCommitsResponse = {
        val filtered = if (endVersion.isPresent) {
          allCommits.filter(_.getVersion <= endVersion.get())
        } else {
          allCommits
        }
        new GetCommitsResponse(filtered.asJava, /* latest */ 3L)
      }
      override def close(): Unit = {}
    }

    // Request commits up to version 1
    val adapter = new UnityCatalogAdapter(tableId, tablePath, clientWithEndVersionFilter(Optional.of(1L)))
    val response = adapter.getCommits(0, Optional.of(java.lang.Long.valueOf(1L)))
    val versions = response.getCommits.asScala.map(_.getVersion)
    assert(versions == Seq(0L, 1L))
  }

  test("getTableId and getTablePath return constructor values") {
    val adapter = new UnityCatalogAdapter(tableId, tablePath, new StubUCClient(
      new GetCommitsResponse(List.empty[Commit].asJava, /* latest */ -1L)))

    assert(adapter.getTableId == tableId)
    assert(adapter.getTablePath == tablePath)
  }

  test("close calls UCClient close") {
    var closeCalled = false
    val client = new UCClient {
      override def getMetastoreId(): String = "meta"
      override def commit(
          tableId: String,
          tableUri: URI,
          commit: Optional[Commit],
          lastKnownBackfilledVersion: Optional[java.lang.Long],
          disown: Boolean,
          newMetadata: Optional[AbstractMetadata],
          newProtocol: Optional[AbstractProtocol]): Unit = {}
      override def getCommits(
          tableId: String,
          tableUri: URI,
          startVersion: Optional[java.lang.Long],
          endVersion: Optional[java.lang.Long]): GetCommitsResponse = {
        new GetCommitsResponse(List.empty[Commit].asJava, -1L)
      }
      override def close(): Unit = { closeCalled = true }
    }

    val adapter = new UnityCatalogAdapter(tableId, tablePath, client)
    adapter.close()
    assert(closeCalled)
  }

  test("close swallows exceptions from UCClient") {
    val client = new UCClient {
      override def getMetastoreId(): String = "meta"
      override def commit(
          tableId: String,
          tableUri: URI,
          commit: Optional[Commit],
          lastKnownBackfilledVersion: Optional[java.lang.Long],
          disown: Boolean,
          newMetadata: Optional[AbstractMetadata],
          newProtocol: Optional[AbstractProtocol]): Unit = {}
      override def getCommits(
          tableId: String,
          tableUri: URI,
          startVersion: Optional[java.lang.Long],
          endVersion: Optional[java.lang.Long]): GetCommitsResponse = {
        new GetCommitsResponse(List.empty[Commit].asJava, -1L)
      }
      override def close(): Unit = { throw new RuntimeException("close failed") }
    }

    val adapter = new UnityCatalogAdapter(tableId, tablePath, client)
    // Should not throw
    adapter.close()
  }

  // Factory method tests

  test("fromConnectionInfo throws on null input") {
    assertThrows[NullPointerException] {
      UnityCatalogAdapter.fromConnectionInfo(null)
    }
  }

  test("fromConnectionInfo creates adapter with correct tableId and tablePath") {
    val info = new UnityCatalogConnectionInfo(
      /* tableId = */ "test-table-id-123",
      /* tablePath = */ "/path/to/delta/table",
      /* endpoint = */ "https://example.net/api",
      /* token = */ "test-token")

    val adapter = UnityCatalogAdapter.fromConnectionInfo(info)
      .asInstanceOf[UnityCatalogAdapter]

    assert(adapter.getTableId == "test-table-id-123")
    assert(adapter.getTablePath == "/path/to/delta/table")
  }
}
