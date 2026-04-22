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

package io.delta.storage.commit.uccommitcoordinator

import java.net.URI
import java.util.{Collections => JColl, Map => JMap, Optional, UUID}

import scala.collection.JavaConverters._

import io.delta.storage.commit.{Commit, CommitFailedException, GetCommitsResponse, TableDescriptor, TableIdentifier}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.storage.commit.uniform.UniformMetadata
import io.unitycatalog.client.delta.model.{CredentialOperation, CredentialsResponse, DeltaCommit, LoadTableResponse, TableUpdate}
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for the DRC routing decision inside [[UCCommitCoordinatorClient]] -- isDRCTable
 * correctly detects tables that were loaded via the DRC path (tableConf carries the DRC
 * catalog/schema/table markers written by DeltaRestTableLoader) and falls through otherwise.
 *
 * These tests cover the precondition that gates commitViaDRC; the end-to-end DRC commit flow
 * (write commit file + call TablesApi.updateTable) has its request-shape coverage in
 * [[UCDeltaRestClientCommitSuite]] (PR 4).
 */
class UCCommitCoordinatorClientDRCRoutingSuite extends AnyFunSuite {

  private val stubUCClient = new NoopUCClient
  private val stubDRC = new NoopDeltaClient

  private def coord(withDRC: Boolean): UCCommitCoordinatorClient = {
    val drcOpt: Optional[UCDeltaClient] =
      if (withDRC) Optional.of(stubDRC.asInstanceOf[UCDeltaClient]) else Optional.empty()
    new UCCommitCoordinatorClient(JColl.emptyMap[String, String](), stubUCClient, drcOpt)
  }

  private def descriptor(tableConf: Map[String, String]): TableDescriptor = {
    val logPath = new Path("s3://bucket/path/_delta_log")
    new TableDescriptor(logPath, Optional.empty[TableIdentifier](), tableConf.asJava)
  }

  test("isDRCTable: DRC client absent -> false even when markers present") {
    val desc = descriptor(Map(
      "io.unitycatalog.drc.catalog" -> "unity",
      "io.unitycatalog.drc.schema" -> "default",
      "io.unitycatalog.drc.table" -> "t",
      "io.unitycatalog.tableId" -> UUID.randomUUID().toString))
    assert(!coord(withDRC = false).isDRCTable(desc))
  }

  test("isDRCTable: DRC client present + all three markers -> true") {
    val desc = descriptor(Map(
      "io.unitycatalog.drc.catalog" -> "unity",
      "io.unitycatalog.drc.schema" -> "default",
      "io.unitycatalog.drc.table" -> "t",
      "io.unitycatalog.tableId" -> UUID.randomUUID().toString))
    assert(coord(withDRC = true).isDRCTable(desc))
  }

  test("isDRCTable: missing catalog marker -> false (falls through to legacy)") {
    val desc = descriptor(Map(
      "io.unitycatalog.drc.schema" -> "default",
      "io.unitycatalog.drc.table" -> "t"))
    assert(!coord(withDRC = true).isDRCTable(desc))
  }

  test("isDRCTable: empty catalog marker -> false") {
    val desc = descriptor(Map(
      "io.unitycatalog.drc.catalog" -> "",
      "io.unitycatalog.drc.schema" -> "default",
      "io.unitycatalog.drc.table" -> "t"))
    assert(!coord(withDRC = true).isDRCTable(desc))
  }

  test("isDRCTable: missing any one of the three markers -> false") {
    Seq("io.unitycatalog.drc.catalog", "io.unitycatalog.drc.schema",
        "io.unitycatalog.drc.table").foreach { missingKey =>
      val full = Map(
        "io.unitycatalog.drc.catalog" -> "unity",
        "io.unitycatalog.drc.schema" -> "default",
        "io.unitycatalog.drc.table" -> "t")
      val desc = descriptor(full - missingKey)
      assert(!coord(withDRC = true).isDRCTable(desc),
        s"expected isDRCTable=false when $missingKey is absent")
    }
  }
}

/** Minimal UCClient stub -- never called in these tests (we don't invoke the commit path). */
private class NoopUCClient extends UCClient {
  override def getMetastoreId(): String = throw new UnsupportedOperationException
  override def commit(
      tableId: String, tableUri: URI, commit: Optional[Commit],
      lastKnownBackfilledVersion: Optional[java.lang.Long], disown: Boolean,
      newMetadata: Optional[AbstractMetadata], newProtocol: Optional[AbstractProtocol],
      uniform: Optional[UniformMetadata]): Unit = throw new UnsupportedOperationException
  override def getCommits(
      tableId: String, tableUri: URI,
      startVersion: Optional[java.lang.Long],
      endVersion: Optional[java.lang.Long]): GetCommitsResponse =
    throw new UnsupportedOperationException
  override def finalizeCreate(
      tableName: String, catalogName: String, schemaName: String,
      storageLocation: String, columns: java.util.List[UCClient.ColumnDef],
      properties: JMap[String, String]): Unit = throw new UnsupportedOperationException
  override def close(): Unit = ()
}

/** Minimal UCDeltaClient stub -- never called in these tests. */
private class NoopDeltaClient extends UCDeltaClient {
  override def loadTable(c: String, s: String, t: String): LoadTableResponse =
    throw new UnsupportedOperationException
  override def getTableCredentials(
      c: String, s: String, t: String, op: CredentialOperation): CredentialsResponse =
    throw new UnsupportedOperationException
  override def commit(
      c: String, s: String, t: String, commit: DeltaCommit, uuid: UUID,
      etag: Optional[String], updates: java.util.List[TableUpdate]): LoadTableResponse =
    throw new UnsupportedOperationException
}
