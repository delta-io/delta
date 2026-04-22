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

import java.util.{Arrays, Collections, HashMap, Optional}

import io.delta.storage.commit.Commit
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}

import org.scalatest.funsuite.AnyFunSuite

/**
 * Construction and null-safety tests for the DTOs exposed by [[UCDeltaClient]]. These
 * lock down the public contract -- null-forbidden fields must reject `null`, Optional
 * fields must accept `Optional.empty()`, unmodifiable collections must reject mutation.
 */
class UCDeltaClientDTOSuite extends AnyFunSuite {

  private def stubMetadata: AbstractMetadata = new AbstractMetadata {
    override def getId: String = "id-1"
    override def getName: String = "t"
    override def getDescription: String = null
    override def getProvider: String = "delta"
    override def getFormatOptions: java.util.Map[String, String] = Collections.emptyMap()
    override def getSchemaString: String = "{\"type\":\"struct\",\"fields\":[]}"
    override def getPartitionColumns: java.util.List[String] = Collections.emptyList()
    override def getConfiguration: java.util.Map[String, String] = Collections.emptyMap()
    override def getCreatedTime: java.lang.Long = 0L
  }

  private def stubProtocol: AbstractProtocol = new AbstractProtocol {
    override def getMinReaderVersion: Int = 1
    override def getMinWriterVersion: Int = 2
    override def getReaderFeatures: java.util.Set[String] = Collections.emptySet()
    override def getWriterFeatures: java.util.Set[String] = Collections.emptySet()
  }

  // --------------------------------------------------------------------------
  // UCLoadTableResponse
  // --------------------------------------------------------------------------

  test("UCLoadTableResponse accepts empty etag (v1 optional)") {
    val resp = new UCLoadTableResponse(
      "uuid-1", stubMetadata, stubProtocol,
      Collections.emptyList[Commit](), 0L, Optional.empty[String]())
    assert(!resp.getEtag.isPresent)
    assert(resp.getLatestTableVersion == 0L)
    assert(resp.toString.contains("etagPresent=false"))
  }

  test("UCLoadTableResponse round-trips present etag and version") {
    val resp = new UCLoadTableResponse(
      "uuid-42", stubMetadata, stubProtocol,
      Collections.emptyList[Commit](), 42L, Optional.of("etag-abc"))
    assert(resp.getEtag.get == "etag-abc")
    assert(resp.getLatestTableVersion == 42L)
    assert(resp.toString.contains("latestTableVersion=42"))
  }

  test("UCLoadTableResponse rejects null required fields") {
    val m = stubMetadata; val p = stubProtocol
    val empty = Collections.emptyList[Commit]()
    intercept[NullPointerException] {
      new UCLoadTableResponse(null, m, p, empty, 0L, Optional.empty())
    }
    intercept[NullPointerException] {
      new UCLoadTableResponse("u", null, p, empty, 0L, Optional.empty())
    }
    intercept[NullPointerException] {
      new UCLoadTableResponse("u", m, null, empty, 0L, Optional.empty())
    }
    intercept[NullPointerException] {
      new UCLoadTableResponse("u", m, p, null, 0L, Optional.empty())
    }
    // etag must be Optional.empty(), not null, so callers cannot silently bypass the
    // intentional v1 optional-etag contract by passing null.
    intercept[NullPointerException] {
      new UCLoadTableResponse("u", m, p, empty, 0L, null)
    }
  }

  test("UCLoadTableResponse.unbackfilledCommits is unmodifiable") {
    val resp = new UCLoadTableResponse(
      "u", stubMetadata, stubProtocol,
      Collections.emptyList[Commit](), 0L, Optional.empty())
    intercept[UnsupportedOperationException] {
      resp.getUnbackfilledCommits.add(null.asInstanceOf[Commit])
    }
  }

  // --------------------------------------------------------------------------
  // UCCreateStagingTableResponse
  // --------------------------------------------------------------------------

  test("UCCreateStagingTableResponse wraps all fields immutably") {
    val creds = Arrays.asList(
      new UCStorageCredential(
        "s3://b/", new HashMap[String, String]() {{ put("k", "v") }}, 0L))
    val resp = new UCCreateStagingTableResponse(
      "table-1", "s3://b/t", creds, stubProtocol,
      new HashMap[String, String]() {{ put("delta.foo", "bar") }})
    assert(resp.getTableId == "table-1")
    assert(resp.getLocation == "s3://b/t")
    assert(resp.getStorageCredentials.size == 1)
    assert(resp.getRequiredProperties.get("delta.foo") == "bar")
    intercept[UnsupportedOperationException] {
      resp.getStorageCredentials.add(null.asInstanceOf[UCStorageCredential])
    }
    intercept[UnsupportedOperationException] {
      resp.getRequiredProperties.put("x", "y")
    }
  }

  test("UCCreateStagingTableResponse rejects null required fields") {
    val empty = Collections.emptyList[UCStorageCredential]()
    val emptyMap = Collections.emptyMap[String, String]()
    intercept[NullPointerException] {
      new UCCreateStagingTableResponse(null, "loc", empty, stubProtocol, emptyMap)
    }
    intercept[NullPointerException] {
      new UCCreateStagingTableResponse("id", null, empty, stubProtocol, emptyMap)
    }
  }

  // --------------------------------------------------------------------------
  // UCListTablesResponse
  // --------------------------------------------------------------------------

  test("UCListTablesResponse round-trips identifiers and page token") {
    val ids = Arrays.asList(new UCTableIdentifier("sch", "t1", "DELTA"))
    val resp = new UCListTablesResponse(ids, Optional.of("next"))
    assert(resp.getIdentifiers.size == 1)
    assert(resp.getIdentifiers.get(0).toString == "sch.t1[DELTA]")
    assert(resp.getNextPageToken.get == "next")
    intercept[UnsupportedOperationException] {
      resp.getIdentifiers.add(null.asInstanceOf[UCTableIdentifier])
    }
  }

  test("UCListTablesResponse accepts empty page token (end of listing)") {
    val resp = new UCListTablesResponse(
      Collections.emptyList[UCTableIdentifier](), Optional.empty[String]())
    assert(!resp.getNextPageToken.isPresent)
  }

  // --------------------------------------------------------------------------
  // UCCredentialsResponse + UCStorageCredential
  // --------------------------------------------------------------------------

  test("UCCredentialsResponse wraps credentials immutably") {
    val c = new UCStorageCredential(
      "s3://bucket/", new HashMap[String, String]() {{ put("token", "x") }}, 1234L)
    val resp = new UCCredentialsResponse(Arrays.asList(c))
    assert(resp.getCredentials.size == 1)
    assert(resp.getCredentials.get(0).getExpiryMillis == 1234L)
    intercept[UnsupportedOperationException] {
      resp.getCredentials.add(null.asInstanceOf[UCStorageCredential])
    }
    intercept[UnsupportedOperationException] {
      resp.getCredentials.get(0).getCredentials.put("k", "v")
    }
  }

  // --------------------------------------------------------------------------
  // UCDomainMetadata
  // --------------------------------------------------------------------------

  test("UCDomainMetadata rejects empty name (fail-loud domain addressing)") {
    intercept[IllegalArgumentException] {
      new UCDomainMetadata("", Collections.emptyMap(), false)
    }
  }

  test("UCDomainMetadata configuration is unmodifiable and toString omits values") {
    val dm = new UCDomainMetadata(
      "delta.clustering",
      new HashMap[String, String]() {{ put("clusteringColumns", "a,b") }},
      false)
    assert(dm.getConfiguration.get("clusteringColumns") == "a,b")
    intercept[UnsupportedOperationException] {
      dm.getConfiguration.put("x", "y")
    }
    // Values may contain PII (literal filter values, etc.); toString shows only keys.
    assert(dm.toString.contains("clusteringColumns"))
    assert(!dm.toString.contains("a,b"))
  }

  // --------------------------------------------------------------------------
  // UCCredentialOperation
  // --------------------------------------------------------------------------

  test("UCCredentialOperation exposes both scopes") {
    assert(UCCredentialOperation.valueOf("READ") == UCCredentialOperation.READ)
    assert(UCCredentialOperation.valueOf("READ_WRITE") == UCCredentialOperation.READ_WRITE)
  }
}
