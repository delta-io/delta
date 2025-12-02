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

  test("getRatifiedCommits sorts by version and converts file status") {
    val commits = List(2L, 0L, 1L).map(commit)
    val adapter = new UnityCatalogAdapter(tableId, tablePath, new StubUCClient(
      new GetCommitsResponse(commits.asJava, /* latest */ 2L)))

    val parsed = adapter.getRatifiedCommits(/* endVersionOpt = */ Optional.empty()).asScala.map(_.getVersion)
    assert(parsed == Seq(0L, 1L, 2L))
    assert(adapter.getLatestRatifiedVersion() == 2L)
  }

  test("getLatestRatifiedVersion maps -1 to 0") {
    val adapter = new UnityCatalogAdapter(tableId, tablePath, new StubUCClient(
      new GetCommitsResponse(List.empty[Commit].asJava, /* latest */ -1L)))

    assert(adapter.getLatestRatifiedVersion() == 0L)
  }

  test("getRatifiedCommits wraps IO failures") {
    val adapter = new UnityCatalogAdapter(tableId, tablePath, new StubUCClient(
      response = null,
      throwOnGet = Some(new IOException("boom"))))

    assertThrows[java.io.UncheckedIOException] {
      adapter.getRatifiedCommits(/* endVersionOpt = */ Optional.empty())
    }
  }

  test("getRatifiedCommits wraps UC coordinator failures") {
    val adapter = new UnityCatalogAdapter(tableId, tablePath, new StubUCClient(
      response = null,
      throwOnGet = Some(new CommitLimitReachedException("boom"))))

    assertThrows[RuntimeException] {
      adapter.getRatifiedCommits(/* endVersionOpt = */ Optional.empty())
    }
  }
}
