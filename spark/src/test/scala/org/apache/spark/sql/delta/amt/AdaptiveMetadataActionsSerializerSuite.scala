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
 * case classes [[ContentRoot]] / [[SidecarType]], and the optional `SidecarFile.type`
 * field.
 */
class AdaptiveMetadataActionsSerializerSuite extends SparkFunSuite {

  private val sampleRoot = ContentRoot(
    path = "metadata/root-abc.parquet",
    sizeInBytes = 4096L,
    version = 1L)

  private val sampleProtocol = Protocol(minReaderVersion = 3, minWriterVersion = 7)
  private val sampleMetadata = Metadata(id = "metadata-id", name = "t")
  private val sampleCheckpointMetadata = CheckpointMetadata(
    version = 1L,
    tags = Map("checkpoint-tag" -> "value"))
  private val sampleDomainMetadatas = Seq(
    DomainMetadata("user.tag", "{}", removed = false),
    DomainMetadata("delta.rowTracking", "{}", removed = false))
  private val sampleTxns = Seq(
    SetTransaction("app-1", 7L, Some(100L)),
    SetTransaction("app-2", 8L, None))
  private val sampleSidecars = Seq(
    dmSidecar(),
    txnSidecar())

  private def sampleRequiredActions: Seq[AMTCheckpointAction] =
    Seq(sampleCheckpointMetadata, sampleRoot, sampleProtocol, sampleMetadata)

  private def sampleFullActions: Seq[AMTCheckpointAction] =
    sampleRequiredActions ++ sampleDomainMetadatas ++ sampleTxns ++ sampleSidecars

  private def sampleRequiredCheckpoint: Checkpoint =
    Checkpoint.fromActions(sampleRequiredActions)

  private def sampleFullCheckpoint: Checkpoint =
    Checkpoint.fromActions(sampleFullActions)

  private def dmSidecar(path: String = "dm.parquet"): SidecarFile =
    SidecarFile(
      path = path,
      sizeInBytes = 1L,
      modificationTime = 0L,
      `type` = Some(SidecarType.Type.DomainMetadata))

  private def txnSidecar(path: String = "txn.parquet"): SidecarFile =
    SidecarFile(
      path = path,
      sizeInBytes = 1L,
      modificationTime = 0L,
      `type` = Some(SidecarType.Type.Txn))

  // ============================================================================================
  // ContentRoot
  // ============================================================================================

  test("ContentRoot: ser-de") {
    val root = ContentRoot(
      path = "metadata/root-abc.parquet",
      sizeInBytes = 4096L,
      version = 1L)
    val json = JsonUtils.toJson(root)
    assert(json ===
      """{"path":"metadata/root-abc.parquet","sizeInBytes":4096,"version":1}""")
    val pretty = JsonUtils.toPrettyJson(root)
    assert(pretty ===
      """{
        |  "path" : "metadata/root-abc.parquet",
        |  "sizeInBytes" : 4096,
        |  "version" : 1
        |}""".stripMargin)
    assert(JsonUtils.fromJson[ContentRoot](json) === root)
    assert(JsonUtils.fromJson[ContentRoot](pretty) === root)
  }

  test("ContentRoot: serde handles Long.MaxValue size") {
    val root = ContentRoot(
      path = "metadata/root-big.parquet",
      sizeInBytes = Long.MaxValue,
      version = Long.MaxValue)
    val json = JsonUtils.toJson(root)
    val roundTripped = JsonUtils.fromJson[ContentRoot](json)
    assert(roundTripped === root)
  }

  test("ContentRoot: tags round-trip and expose typed accessors") {
    val root = ContentRoot(
      path = "metadata/root-abc.parquet",
      sizeInBytes = 4096L,
      version = 7L,
      isIncremental = true,
      lastManifestCommitWithFullRewrite = 7L,
      numLeaves = 3L)
    assert(root.version === 7L)
    assert(root.isIncremental === Some(true))
    assert(root.lastManifestCommitWithFullRewrite === Some(7L))
    assert(root.numLeaves === Some(3L))
    val roundTripped = JsonUtils.fromJson[ContentRoot](JsonUtils.toJson(root))
    assert(roundTripped === root)
    assert(roundTripped.version === 7L)
    assert(roundTripped.isIncremental === Some(true))
    assert(roundTripped.lastManifestCommitWithFullRewrite === Some(7L))
    assert(roundTripped.numLeaves === Some(3L))
  }

  test("ContentRoot: accessors are None when tags are absent") {
    assert(sampleRoot.isIncremental.isEmpty)
    assert(sampleRoot.lastManifestCommitWithFullRewrite.isEmpty)
    assert(sampleRoot.numLeaves.isEmpty)
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

  test("SidecarFile.type: each SidecarType value round-trips through JSON") {
    Seq(SidecarType.Type.DomainMetadata, SidecarType.Type.Txn)
      .foreach { value =>
        val sf = SidecarFile(
          path = s"$value.parquet",
          sizeInBytes = 1L,
          modificationTime = 0L,
          `type` = Some(value))
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
        assert(parsed.`type`.contains(value))
      }
  }

  test("SidecarFile.type: absent from JSON when None") {
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

  test("SidecarFile rejects unknown type values at construction") {
    Seq("systemDomainMetadata", "bogus", "").foreach { bad =>
      val ex = intercept[IllegalArgumentException] {
        SidecarFile(
          path = "x.parquet",
          sizeInBytes = 1L,
          modificationTime = 0L,
          `type` = Some(bad))
      }
      assert(ex.getMessage.toLowerCase(java.util.Locale.ROOT).contains("unknown"))
    }
  }

  test("SidecarFile accepts None type and known SidecarType values") {
    SidecarFile(path = "x.parquet", sizeInBytes = 1L, modificationTime = 0L)
    Seq(SidecarType.Type.DomainMetadata, SidecarType.Type.Txn).foreach { value =>
      SidecarFile(
        path = "x.parquet",
        sizeInBytes = 1L,
        modificationTime = 0L,
        `type` = Some(value))
    }
  }

  // ============================================================================================
  // AMTCheckpointSingleAction
  // ============================================================================================

  Seq[(String, AMTCheckpointAction)](
    "checkpointMetadata" -> sampleCheckpointMetadata,
    "contentRoot" -> sampleRoot,
    "protocol" -> sampleProtocol,
    "metaData" -> sampleMetadata,
    "domainMetadata" -> sampleDomainMetadatas.head,
    "txn" -> sampleTxns.head,
    "sidecar" -> sampleSidecars.head
  ).foreach { case (fieldName, action) =>
    test(s"AMTCheckpointSingleAction: $fieldName wrap, unwrap, and JSON round-trip") {
      val wrapped = action.wrapAsAMTCheckpointSingleAction
      assert(wrapped.unwrap === action)
      val json = JsonUtils.toJson(wrapped)
      val node = JsonUtils.mapper.readTree(json)
      assert(node.size() === 1)
      assert(node.has(fieldName))
      assert(node.get(fieldName) === JsonUtils.mapper.readTree(JsonUtils.toJson(action)))
      assert(JsonUtils.fromJson[AMTCheckpointSingleAction](json) === wrapped)
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
        |  "checkpoint" : [ {
        |    "checkpointMetadata" : {
        |      "version" : 1
        |    }
        |  }, {
        |    "contentRoot" : {
        |      "path" : "metadata/root-abc.parquet",
        |      "sizeInBytes" : 4096,
        |      "version" : 1
        |    }
        |  }, {
        |    "protocol" : {
        |      "minReaderVersion" : 3,
        |      "minWriterVersion" : 7,
        |      "readerFeatures" : [ ],
        |      "writerFeatures" : [ ]
        |    }
        |  }, {
        |    "metaData" : {
        |      "id" : "metadata-id",
        |      "name" : "t",
        |      "format" : {
        |        "provider" : "parquet",
        |        "options" : { }
        |      },
        |      "partitionColumns" : [ ],
        |      "configuration" : { }
        |    }
        |  } ]
        |}""".stripMargin)
    val parsed = Action.fromJson(cp.json).asInstanceOf[Checkpoint]
    assert(parsed === cp)
  }

  test("Checkpoint: routes through SingleAction.unwrap") {
    val checkpoint = sampleRequiredCheckpoint
    val wrapped = checkpoint.wrap
    assert(wrapped.checkpoint === checkpoint.actions)
    assert(wrapped.unwrap === checkpoint)
  }

  test("Checkpoint: ser-de round-trips as a bare action array") {
    val checkpoint = sampleFullCheckpoint
    val json = JsonUtils.toJson(checkpoint)
    val node = JsonUtils.mapper.readTree(json)
    assert(node.isArray)
    assert(node === JsonUtils.mapper.readTree(JsonUtils.toJson(checkpoint.actions)))
    assert(node === JsonUtils.mapper.readTree(checkpoint.json).get("checkpoint"))
    val roundTripped = JsonUtils.fromJson[Checkpoint](json)
    assert(roundTripped === checkpoint)
    assert(roundTripped.checkpointMetadata.tags === sampleCheckpointMetadata.tags)
  }

  Seq(
    "{}",
    """{"unknownAction":{"value":1}}"""
  ).foreach { unknownJson =>
    test(s"Checkpoint: ignores an unrecognized envelope $unknownJson") {
      // Unrecognized JSON envelope unwraps to null.
      val unknownAction = JsonUtils.fromJson[AMTCheckpointSingleAction](unknownJson)
      assert(unknownAction.unwrap === null)

      // Checkpoint JSON with unrecognized envelope deserializes to a checkpoint with the
      // unrecognized action, with other actions intact.
      val checkpoint = sampleFullCheckpoint
      val actionsJson = JsonUtils.toJson(checkpoint.actions)
      val mixedJson = actionsJson.dropRight(1) + s",$unknownJson]"
      val expectedMixedCheckpoint = checkpoint.copy(
        actions = checkpoint.actions :+ AMTCheckpointSingleAction())
      val deserialized = JsonUtils.fromJson[Checkpoint](mixedJson)
      assert(deserialized === expectedMixedCheckpoint)
      assert(deserialized.checkpointMetadata === sampleCheckpointMetadata)
      assert(deserialized.contentRoot === sampleRoot)
      assert(deserialized.protocol === sampleProtocol)
      assert(deserialized.metaData === sampleMetadata)
      assert(deserialized.domainMetadata === sampleDomainMetadatas)
      assert(deserialized.txns === sampleTxns)
      assert(deserialized.sidecars === sampleSidecars)
      assert(Action.fromJson(s"""{"checkpoint":$mixedJson}""") === expectedMixedCheckpoint)

      // The required actions invariant is still maintained during deserialization.
      val e = intercept[IllegalArgumentException] {
        JsonUtils.fromJson[Checkpoint](s"[$unknownJson]")
      }
      assert(e.getMessage.contains("exactly one CheckpointMetadata action, found 0"))
    }
  }

  // ============================================================================================
  // Checkpoint: round-trip across field shapes
  // ============================================================================================

  test("Checkpoint: extracts reordered singletons and interleaved repeated actions") {
    val reorderedActions = Seq[AMTCheckpointAction](
      sampleTxns.head,
      sampleMetadata,
      sampleDomainMetadatas.last,
      sampleSidecars.last,
      sampleProtocol,
      sampleCheckpointMetadata,
      sampleTxns.last,
      sampleRoot,
      sampleSidecars.head,
      sampleDomainMetadatas.head)
    val reorderedCheckpoint = Checkpoint.fromActions(reorderedActions)
    assert(reorderedCheckpoint.actions.map(_.unwrap) === reorderedActions)

    assert(reorderedCheckpoint.checkpointMetadata === sampleCheckpointMetadata)
    assert(reorderedCheckpoint.version === sampleCheckpointMetadata.version)
    assert(reorderedCheckpoint.contentRoot === sampleRoot)
    assert(reorderedCheckpoint.protocol === sampleProtocol)
    assert(reorderedCheckpoint.metaData === sampleMetadata)

    assert(reorderedCheckpoint.domainMetadata === sampleDomainMetadatas.reverse)
    assert(reorderedCheckpoint.txns === sampleTxns) // order is preserved
    assert(reorderedCheckpoint.sidecars === sampleSidecars.reverse)

    assert(Action.fromJson(reorderedCheckpoint.json) === reorderedCheckpoint)
  }

  test("Checkpoint: round-trips the full inline snapshot") {
    val protocol = Protocol(3, 7)
      .withReaderFeatures(Seq("deletionVectors", "v2Checkpoint"))
      .withWriterFeatures(Seq("deletionVectors", "v2Checkpoint", "rowTracking"))
    val dm = Seq(
      DomainMetadata(domain = "delta.rowTracking", configuration = "{}", removed = false),
      DomainMetadata(domain = "user.tag", configuration = "{\"k\":\"v\"}", removed = false))
    val cp = Checkpoint(
      version = 41L,
      contentRoot = sampleRoot.copy(version = 41L),
      protocol = protocol,
      metaData = sampleMetadata,
      domainMetadata = dm,
      txns = sampleTxns,
      sidecars = Seq.empty)
    assert(JsonUtils.toPrettyJson(cp.wrap) ===
      """{
        |  "checkpoint" : [ {
        |    "checkpointMetadata" : {
        |      "version" : 41
        |    }
        |  }, {
        |    "contentRoot" : {
        |      "path" : "metadata/root-abc.parquet",
        |      "sizeInBytes" : 4096,
        |      "version" : 41
        |    }
        |  }, {
        |    "protocol" : {
        |      "minReaderVersion" : 3,
        |      "minWriterVersion" : 7,
        |      "readerFeatures" : [ "deletionVectors", "v2Checkpoint" ],
        |      "writerFeatures" : [ "deletionVectors", "v2Checkpoint", "rowTracking" ]
        |    }
        |  }, {
        |    "metaData" : {
        |      "id" : "metadata-id",
        |      "name" : "t",
        |      "format" : {
        |        "provider" : "parquet",
        |        "options" : { }
        |      },
        |      "partitionColumns" : [ ],
        |      "configuration" : { }
        |    }
        |  }, {
        |    "domainMetadata" : {
        |      "domain" : "delta.rowTracking",
        |      "configuration" : "{}",
        |      "removed" : false
        |    }
        |  }, {
        |    "domainMetadata" : {
        |      "domain" : "user.tag",
        |      "configuration" : "{\"k\":\"v\"}",
        |      "removed" : false
        |    }
        |  }, {
        |    "txn" : {
        |      "appId" : "app-1",
        |      "version" : 7,
        |      "lastUpdated" : 100
        |    }
        |  }, {
        |    "txn" : {
        |      "appId" : "app-2",
        |      "version" : 8
        |    }
        |  } ]
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
      sidecars = sampleSidecars)
    assert(JsonUtils.toPrettyJson(cp.wrap) ===
      """{
        |  "checkpoint" : [ {
        |    "checkpointMetadata" : {
        |      "version" : 1
        |    }
        |  }, {
        |    "contentRoot" : {
        |      "path" : "metadata/root-abc.parquet",
        |      "sizeInBytes" : 4096,
        |      "version" : 1
        |    }
        |  }, {
        |    "protocol" : {
        |      "minReaderVersion" : 3,
        |      "minWriterVersion" : 7,
        |      "readerFeatures" : [ ],
        |      "writerFeatures" : [ ]
        |    }
        |  }, {
        |    "metaData" : {
        |      "id" : "metadata-id",
        |      "name" : "t",
        |      "format" : {
        |        "provider" : "parquet",
        |        "options" : { }
        |      },
        |      "partitionColumns" : [ ],
        |      "configuration" : { }
        |    }
        |  }, {
        |    "sidecar" : {
        |      "path" : "dm.parquet",
        |      "sizeInBytes" : 1,
        |      "modificationTime" : 0,
        |      "type" : "domainMetadata"
        |    }
        |  }, {
        |    "sidecar" : {
        |      "path" : "txn.parquet",
        |      "sizeInBytes" : 1,
        |      "modificationTime" : 0,
        |      "type" : "txn"
        |    }
        |  } ]
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
      sidecars = sampleSidecars)
    val parsed = Action.fromJson(cp.json).asInstanceOf[Checkpoint]
    assert(parsed === cp)
    assert(parsed.domainMetadata === dm)
    assert(parsed.txns.isEmpty)
    assert(parsed.sidecars.length === 2)
  }

  // ============================================================================================
  // Checkpoint: validation
  // ============================================================================================

  for {
    requiredAction <- sampleRequiredActions
    count <- Seq(0, 2)
  } {
    val actionName = requiredAction.getClass.getSimpleName
    test(s"Checkpoint: rejects $count $actionName actions at construction and JSON decoding") {
      val actions = sampleFullActions.filterNot(_ == requiredAction) ++
        Seq.fill(count)(requiredAction)
      val expectedMessage = s"exactly one $actionName action, found $count"
      val ex = intercept[IllegalArgumentException] {
        Checkpoint.fromActions(actions)
      }
      assert(ex.getMessage.contains(expectedMessage))
      val jsonEx = intercept[IllegalArgumentException] {
        val wrapped = actions.map(_.wrapAsAMTCheckpointSingleAction)
        JsonUtils.fromJson[Checkpoint](JsonUtils.toJson(wrapped))
      }
      assert(jsonEx.getMessage.contains(expectedMessage))
    }
  }

  test("Checkpoint: rejects a null action collection") {
    val ex = intercept[IllegalArgumentException] {
      Checkpoint(actions = null)
    }
    assert(ex.getMessage.contains("Checkpoint actions must not be null"))
  }

  test("Checkpoint: rejects null entries at construction and JSON decoding") {
    val actions = sampleFullCheckpoint.actions :+ null
    val ex = intercept[IllegalArgumentException] {
      Checkpoint(actions)
    }
    assert(ex.getMessage.contains("Checkpoint actions must not contain null entries"))
    val jsonEx = intercept[IllegalArgumentException] {
      JsonUtils.fromJson[Checkpoint](JsonUtils.toJson(actions))
    }
    assert(jsonEx.getMessage.contains("Checkpoint actions must not contain null entries"))
  }

  test("Checkpoint: rejects sidecar without type") {
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
    assert(ex.getMessage.contains("must have a type"))
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
    assert(parsed.sidecars.forall(_.`type`.contains(SidecarType.Type.DomainMetadata)))
  }

  test("Checkpoint: contentRoot.version may lag checkpoint.version") {
    val cp = Checkpoint(
      version = 10L,
      contentRoot = sampleRoot.copy(version = 5L),
      protocol = sampleProtocol,
      metaData = sampleMetadata,
      domainMetadata = Seq.empty,
      txns = Seq.empty,
      sidecars = Seq.empty)
    val parsed = Action.fromJson(cp.json).asInstanceOf[Checkpoint]
    assert(parsed.contentRoot.version === 5L)
    assert(parsed.version === 10L)
  }

  test("Checkpoint: rejects contentRoot.version ahead of checkpoint.version") {
    val ex = intercept[IllegalArgumentException] {
      Checkpoint(
        version = 1L,
        contentRoot = sampleRoot.copy(version = 2L),
        protocol = sampleProtocol,
        metaData = sampleMetadata,
        domainMetadata = Seq.empty,
        txns = Seq.empty,
        sidecars = Seq.empty)
    }
    assert(ex.getMessage.contains("contentRoot.version"))
  }
}
