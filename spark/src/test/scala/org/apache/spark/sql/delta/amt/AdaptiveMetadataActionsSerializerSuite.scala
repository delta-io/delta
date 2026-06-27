/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.amt

import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.SparkFunSuite

/**
 * JSON round-trip and invariant tests for the action-schema additions introduced for the
 * `adaptiveMetadata-preview` feature: the new top-level [[Checkpoint]] action, the helper
 * case classes [[ContentRoot]] / [[SidecarType]], and the optional `SidecarFile.sidecarType`
 * field.
 */
class AdaptiveMetadataActionsSerializerSuite extends SparkFunSuite {

  private val sampleRoot = ContentRoot(
    path = "metadata/root-abc.parquet",
    sizeInBytes = 4096L)

  private val sampleProtocol = Protocol(minReaderVersion = 3, minWriterVersion = 7)
  private val sampleMetadata = Metadata(id = "metadata-id", name = "t")

  private def dmSidecar(path: String = "dm.parquet"): SidecarFile =
    SidecarFile(
      path = path,
      sizeInBytes = 1L,
      modificationTime = 0L,
      sidecarType = Some(SidecarType.Type.DomainMetadata))

  private def txnSidecar(path: String = "txn.parquet"): SidecarFile =
    SidecarFile(
      path = path,
      sizeInBytes = 1L,
      modificationTime = 0L,
      sidecarType = Some(SidecarType.Type.Txn))

  // ============================================================================================
  // ContentRoot
  // ============================================================================================

  test("ContentRoot: ser-de") {
    val root = ContentRoot(
      path = "metadata/root-abc.parquet",
      sizeInBytes = 4096L)
    val json = JsonUtils.toJson(root)
    assert(json ===
      """{"path":"metadata/root-abc.parquet","sizeInBytes":4096}""")
    val pretty = JsonUtils.toPrettyJson(root)
    assert(pretty ===
      """{
        |  "path" : "metadata/root-abc.parquet",
        |  "sizeInBytes" : 4096
        |}""".stripMargin)
    assert(JsonUtils.fromJson[ContentRoot](json) === root)
    assert(JsonUtils.fromJson[ContentRoot](pretty) === root)
  }

  test("ContentRoot: serde handles Long.MaxValue size") {
    val root = ContentRoot(
      path = "metadata/root-big.parquet",
      sizeInBytes = Long.MaxValue)
    val json = JsonUtils.toJson(root)
    val roundTripped = JsonUtils.fromJson[ContentRoot](json)
    assert(roundTripped === root)
  }

  // ============================================================================================
  // SidecarType enum
  // ============================================================================================

  test("SidecarType: check values") {
    assert(SidecarType.Type.DomainMetadata === "domainMetadata")
    assert(SidecarType.Type.Txn === "txn")
  }

  test("SidecarType.all contains exactly the two known values") {
    assert(SidecarType.all === Set(
      SidecarType.Type.DomainMetadata,
      SidecarType.Type.Txn))
  }

  test("SidecarType.validate accepts known values and rejects unknown") {
    Seq(SidecarType.Type.DomainMetadata, SidecarType.Type.Txn)
      .foreach { v => assert(SidecarType.validate(v) === v) }
    Seq("bogus", "systemDomainMetadata").foreach { bad =>
      val ex = intercept[IllegalArgumentException] { SidecarType.validate(bad) }
      assert(ex.getMessage.toLowerCase(java.util.Locale.ROOT).contains("unknown"))
    }
  }

  test("SidecarFile.sidecarType: each SidecarType value round-trips through JSON") {
    Seq(SidecarType.Type.DomainMetadata, SidecarType.Type.Txn)
      .foreach { value =>
        val sf = SidecarFile(
          path = s"$value.parquet",
          sizeInBytes = 1L,
          modificationTime = 0L,
          sidecarType = Some(value))
        assert(sf.json ===
          s"""{"sidecar":{"path":"$value.parquet","sizeInBytes":1,""" +
            s""""modificationTime":0,"type":"$value"}}""")
        assert(JsonUtils.toPrettyJson(sf.wrap) ===
          s"""{
             |  "sidecar" : {
             |    "path" : "$value.parquet",
             |    "sizeInBytes" : 1,
             |    "modificationTime" : 0,
             |    "type" : "$value"
             |  }
             |}""".stripMargin)
        val parsed = Action.fromJson(sf.json).asInstanceOf[SidecarFile]
        assert(parsed === sf)
        assert(parsed.sidecarType.contains(value))
      }
  }

  test("SidecarFile.sidecarType: absent from JSON when None") {
    val sf = SidecarFile(path = "s.parquet", sizeInBytes = 1L, modificationTime = 0L)
    assert(sf.json === """{"sidecar":{"path":"s.parquet","sizeInBytes":1,"modificationTime":0}}""")
    assert(JsonUtils.toPrettyJson(sf.wrap) ===
      """{
        |  "sidecar" : {
        |    "path" : "s.parquet",
        |    "sizeInBytes" : 1,
        |    "modificationTime" : 0
        |  }
        |}""".stripMargin)
    assert(Action.fromJson(sf.json) === sf)
  }

  test("SidecarFile rejects unknown sidecarType values at construction") {
    Seq("systemDomainMetadata", "bogus", "").foreach { bad =>
      val ex = intercept[IllegalArgumentException] {
        SidecarFile(
          path = "x.parquet",
          sizeInBytes = 1L,
          modificationTime = 0L,
          sidecarType = Some(bad))
      }
      assert(ex.getMessage.toLowerCase(java.util.Locale.ROOT).contains("unknown"))
    }
  }

  test("SidecarFile accepts None sidecarType and known SidecarType values") {
    SidecarFile(path = "x.parquet", sizeInBytes = 1L, modificationTime = 0L)
    Seq(SidecarType.Type.DomainMetadata, SidecarType.Type.Txn).foreach { value =>
      SidecarFile(
        path = "x.parquet",
        sizeInBytes = 1L,
        modificationTime = 0L,
        sidecarType = Some(value))
    }
  }

  // ============================================================================================
  // Checkpoint: ser-de
  // ============================================================================================

  test("Checkpoint: ser-de") {
    val cp = Checkpoint(
      version = 1L,
      contentRoot = sampleRoot,
      protocol = sampleProtocol,
      metaData = sampleMetadata,
      domainMetadata = Seq.empty,
      txns = Seq.empty,
      sidecars = Seq.empty)
    assert(JsonUtils.toPrettyJson(cp.wrap) ===
      """{
        |  "checkpoint" : {
        |    "version" : 1,
        |    "contentRoot" : {
        |      "path" : "metadata/root-abc.parquet",
        |      "sizeInBytes" : 4096
        |    },
        |    "protocol" : {
        |      "minReaderVersion" : 3,
        |      "minWriterVersion" : 7,
        |      "readerFeatures" : [ ],
        |      "writerFeatures" : [ ]
        |    },
        |    "metaData" : {
        |      "id" : "metadata-id",
        |      "name" : "t",
        |      "format" : {
        |        "provider" : "parquet",
        |        "options" : { }
        |      },
        |      "partitionColumns" : [ ],
        |      "configuration" : { }
        |    },
        |    "domainMetadata" : [ ],
        |    "txns" : [ ],
        |    "sidecars" : [ ]
        |  }
        |}""".stripMargin)
    val parsed = Action.fromJson(cp.json).asInstanceOf[Checkpoint]
    assert(parsed === cp)
  }

  test("Checkpoint: routes through SingleAction.unwrap") {
    val cp = Checkpoint(
      version = 1L,
      contentRoot = sampleRoot,
      protocol = sampleProtocol,
      metaData = sampleMetadata,
      domainMetadata = Seq.empty,
      txns = Seq.empty,
      sidecars = Seq.empty)
    val wrapped = cp.wrap
    assert(wrapped.checkpoint === cp)
    assert(wrapped.unwrap === cp)
  }

  // ============================================================================================
  // Checkpoint: round-trip across field shapes
  // ============================================================================================

  test("Checkpoint: round-trips the full inline snapshot") {
    val protocol = Protocol(3, 7)
      .withReaderFeatures(Seq("deletionVectors", "v2Checkpoint"))
      .withWriterFeatures(Seq("deletionVectors", "v2Checkpoint", "rowTracking"))
    val dm = Seq(
      DomainMetadata(domain = "delta.rowTracking", configuration = "{}", removed = false),
      DomainMetadata(domain = "user.tag", configuration = "{\"k\":\"v\"}", removed = false))
    val txns = Seq(
      SetTransaction(appId = "app-1", version = 7L, lastUpdated = Some(100L)),
      SetTransaction(appId = "app-2", version = 8L, lastUpdated = None))
    val cp = Checkpoint(
      version = 41L,
      contentRoot = sampleRoot,
      protocol = protocol,
      metaData = sampleMetadata,
      domainMetadata = dm,
      txns = txns,
      sidecars = Seq.empty)
    assert(JsonUtils.toPrettyJson(cp.wrap) ===
      """{
        |  "checkpoint" : {
        |    "version" : 41,
        |    "contentRoot" : {
        |      "path" : "metadata/root-abc.parquet",
        |      "sizeInBytes" : 4096
        |    },
        |    "protocol" : {
        |      "minReaderVersion" : 3,
        |      "minWriterVersion" : 7,
        |      "readerFeatures" : [ "deletionVectors", "v2Checkpoint" ],
        |      "writerFeatures" : [ "deletionVectors", "v2Checkpoint", "rowTracking" ]
        |    },
        |    "metaData" : {
        |      "id" : "metadata-id",
        |      "name" : "t",
        |      "format" : {
        |        "provider" : "parquet",
        |        "options" : { }
        |      },
        |      "partitionColumns" : [ ],
        |      "configuration" : { }
        |    },
        |    "domainMetadata" : [ {
        |      "domain" : "delta.rowTracking",
        |      "configuration" : "{}",
        |      "removed" : false
        |    }, {
        |      "domain" : "user.tag",
        |      "configuration" : "{\"k\":\"v\"}",
        |      "removed" : false
        |    } ],
        |    "txns" : [ {
        |      "appId" : "app-1",
        |      "version" : 7,
        |      "lastUpdated" : 100
        |    }, {
        |      "appId" : "app-2",
        |      "version" : 8
        |    } ],
        |    "sidecars" : [ ]
        |  }
        |}""".stripMargin)
    val parsed = Action.fromJson(cp.json).asInstanceOf[Checkpoint]
    assert(parsed === cp)
  }

  test("Checkpoint: round-trips with domainMetadata and txns carried via sidecars") {
    val cp = Checkpoint(
      version = 1L,
      contentRoot = sampleRoot,
      protocol = sampleProtocol,
      metaData = sampleMetadata,
      domainMetadata = Seq.empty,
      txns = Seq.empty,
      sidecars = Seq(dmSidecar(), txnSidecar()))
    assert(JsonUtils.toPrettyJson(cp.wrap) ===
      """{
        |  "checkpoint" : {
        |    "version" : 1,
        |    "contentRoot" : {
        |      "path" : "metadata/root-abc.parquet",
        |      "sizeInBytes" : 4096
        |    },
        |    "protocol" : {
        |      "minReaderVersion" : 3,
        |      "minWriterVersion" : 7,
        |      "readerFeatures" : [ ],
        |      "writerFeatures" : [ ]
        |    },
        |    "metaData" : {
        |      "id" : "metadata-id",
        |      "name" : "t",
        |      "format" : {
        |        "provider" : "parquet",
        |        "options" : { }
        |      },
        |      "partitionColumns" : [ ],
        |      "configuration" : { }
        |    },
        |    "domainMetadata" : [ ],
        |    "txns" : [ ],
        |    "sidecars" : [ {
        |      "path" : "dm.parquet",
        |      "sizeInBytes" : 1,
        |      "modificationTime" : 0,
        |      "type" : "domainMetadata"
        |    }, {
        |      "path" : "txn.parquet",
        |      "sizeInBytes" : 1,
        |      "modificationTime" : 0,
        |      "type" : "txn"
        |    } ]
        |  }
        |}""".stripMargin)
    val parsed = Action.fromJson(cp.json).asInstanceOf[Checkpoint]
    assert(parsed === cp)
  }

  test("Checkpoint: round-trips with mixed inline-and-sidecar") {
    val dm = Seq(
      DomainMetadata(domain = "delta.rowTracking", configuration = "{}", removed = false))
    val cp = Checkpoint(
      version = 1L,
      contentRoot = sampleRoot,
      protocol = sampleProtocol,
      metaData = sampleMetadata,
      domainMetadata = dm,
      txns = Seq.empty,
      sidecars = Seq(dmSidecar(), txnSidecar()))
    val parsed = Action.fromJson(cp.json).asInstanceOf[Checkpoint]
    assert(parsed === cp)
    assert(parsed.domainMetadata === dm)
    assert(parsed.txns.isEmpty)
    assert(parsed.sidecars.length === 2)
  }

  test("Checkpoint: rejects sidecar without sidecarType") {
    val untyped = SidecarFile(path = "x.parquet", sizeInBytes = 1L, modificationTime = 0L)
    val ex = intercept[IllegalArgumentException] {
      Checkpoint(
        version = 1L,
        contentRoot = sampleRoot,
        protocol = sampleProtocol,
        metaData = sampleMetadata,
        domainMetadata = Seq.empty,
        txns = Seq.empty,
        sidecars = Seq(untyped))
    }
    assert(ex.getMessage.contains("sidecarType"))
  }

  test("Checkpoint: round-trips multiple sidecars of the same type") {
    val cp = Checkpoint(
      version = 1L,
      contentRoot = sampleRoot,
      protocol = sampleProtocol,
      metaData = sampleMetadata,
      domainMetadata = Seq.empty,
      txns = Seq.empty,
      sidecars = Seq(dmSidecar("dm-1.parquet"), dmSidecar("dm-2.parquet")))
    val parsed = Action.fromJson(cp.json).asInstanceOf[Checkpoint]
    assert(parsed === cp)
    assert(parsed.sidecars.length === 2)
    assert(parsed.sidecars.forall(_.sidecarType.contains(SidecarType.Type.DomainMetadata)))
  }
}
