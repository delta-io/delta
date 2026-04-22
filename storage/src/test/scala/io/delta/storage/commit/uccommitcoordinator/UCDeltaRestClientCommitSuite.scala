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

import java.util.{Collections => JColl, Optional, UUID}

import scala.collection.JavaConverters._

import io.unitycatalog.client.delta.model.{
  AddCommitUpdate,
  AssertEtag,
  AssertTableUUID,
  DeltaCommit,
  SetPropertiesUpdate,
  TableUpdate
}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for {@link UCDeltaRestClient#buildCommitRequest}. The method is package-private
 * so we can assert the shape of the generated {@code UpdateTableRequest} (requirements + updates
 * composition) without standing up an HTTP server.
 *
 * Field sets validated here are derived from the UC DRC OpenAPI spec at UC SHA ae4bcf6.
 * Re-verify these shape assertions when the UC server implements the
 * {@code POST /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}} handler.
 */
class UCDeltaRestClientCommitSuite extends AnyFunSuite {

  private val tableUuid = UUID.fromString("11111111-2222-3333-4444-555555555555")

  private def commit(): DeltaCommit = new DeltaCommit()
    .version(7L)
    .timestamp(1_700_000_000_000L)
    .fileName("_staged_commits/0000000000000000007.some-uuid.json")
    .fileSize(1234L)
    .fileModificationTimestamp(1_700_000_000_100L)

  test("buildCommitRequest: without etag, requirements = [AssertTableUUID]") {
    val req = UCDeltaRestClient.buildCommitRequest(
      commit(), tableUuid, Optional.empty[String](), JColl.emptyList[TableUpdate]())

    val reqs = req.getRequirements.asScala
    assert(reqs.length == 1)
    reqs.head match {
      case a: AssertTableUUID => assert(a.getUuid == tableUuid)
      case other => fail(s"Expected AssertTableUUID, got ${other.getClass.getName}")
    }

    val updates = req.getUpdates.asScala
    assert(updates.length == 1)
    updates.head match {
      case u: AddCommitUpdate =>
        val c = u.getCommit
        assert(c.getVersion == 7L)
        assert(c.getFileName.endsWith(".some-uuid.json"))
        assert(c.getFileSize == 1234L)
      case other => fail(s"Expected AddCommitUpdate, got ${other.getClass.getName}")
    }
  }

  test("buildCommitRequest: with etag, requirements = [AssertTableUUID, AssertEtag]") {
    val req = UCDeltaRestClient.buildCommitRequest(
      commit(), tableUuid, Optional.of("CAES-etag"), JColl.emptyList[TableUpdate]())

    val reqs = req.getRequirements.asScala
    assert(reqs.length == 2)
    reqs(0) match {
      case a: AssertTableUUID => assert(a.getUuid == tableUuid)
      case other => fail(s"Expected AssertTableUUID, got ${other.getClass.getName}")
    }
    reqs(1) match {
      case e: AssertEtag => assert(e.getEtag == "CAES-etag")
      case other => fail(s"Expected AssertEtag, got ${other.getClass.getName}")
    }
  }

  test("buildCommitRequest rejects null arguments") {
    intercept[NullPointerException] {
      UCDeltaRestClient.buildCommitRequest(
        null, tableUuid, Optional.empty(), JColl.emptyList[TableUpdate]())
    }
    intercept[NullPointerException] {
      UCDeltaRestClient.buildCommitRequest(
        commit(), null, Optional.empty(), JColl.emptyList[TableUpdate]())
    }
    intercept[NullPointerException] {
      UCDeltaRestClient.buildCommitRequest(
        commit(), tableUuid, null, JColl.emptyList[TableUpdate]())
    }
    intercept[NullPointerException] {
      UCDeltaRestClient.buildCommitRequest(
        commit(), tableUuid, Optional.empty(), null)
    }
  }

  test("buildCommitRequest appends metadata updates AFTER the AddCommitUpdate") {
    val setProps: TableUpdate = new SetPropertiesUpdate()
      .updates(java.util.Collections.singletonMap("delta.enableChangeDataFeed", "true"))
    val req = UCDeltaRestClient.buildCommitRequest(
      commit(),
      tableUuid,
      Optional.empty[String](),
      java.util.Arrays.asList(setProps))
    val updates = req.getUpdates.asScala
    assert(updates.length == 2)
    assert(updates(0).isInstanceOf[AddCommitUpdate])
    assert(updates(1).isInstanceOf[SetPropertiesUpdate])
    val sp = updates(1).asInstanceOf[SetPropertiesUpdate]
    assert(sp.getUpdates.asScala.toMap ===
      Map("delta.enableChangeDataFeed" -> "true"))
  }
}
