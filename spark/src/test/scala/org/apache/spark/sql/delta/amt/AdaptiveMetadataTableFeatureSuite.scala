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

import java.util.Locale

import org.apache.spark.sql.delta.{
  AdaptiveMetadataTableFeature,
  CatalogOwnedTableFeature,
  ColumnMappingTableFeature,
  DeletionVectorsTableFeature,
  DeltaAnalysisException,
  DeltaConfigs,
  DeltaLog,
  DeltaOperations,
  DeltaTableFeatureException,
  DeltaTableProvider,
  DeltaUnsupportedOperationException,
  DomainMetadataTableFeature,
  FeatureAutomaticallyEnabledByMetadata,
  RemovableFeature,
  RowTrackingFeature,
  TableFeature,
  V2CheckpointTableFeature
}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils._
import org.apache.spark.sql.delta.coordinatedcommits.CatalogOwnedTestBaseSuite
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier

class AdaptiveMetadataTableFeatureSuite
  extends QueryTest
  with DeltaSQLTestUtils
  with DeltaSQLCommandTest
  with CatalogOwnedTestBaseSuite
  with DeltaTableProvider {

  // Register the in-memory commit coordinator so catalog-managed tables can be created locally.
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(2)

  /**
   * Creates a table by name with the AMT feature plus any extra table properties. `command` is
   * the creating statement, e.g. `CREATE OR REPLACE TABLE` to replace an existing table.
   */
  private def createAdaptiveMetadataTable(
      tableName: String,
      extraProps: Map[String, String] = Map.empty,
      command: String = "CREATE TABLE"): Unit = {
    val props = (Map(propertyKey(AdaptiveMetadataTableFeature) -> FEATURE_PROP_SUPPORTED) ++
      extraProps).map { case (k, v) => s"'$k' = '$v'" }.mkString(", ")
    sql(s"$command $tableName (id INT) USING $tableProvider TBLPROPERTIES ($props)")
  }

  private def metadataOf(tableName: String) =
    DeltaLog.forTableWithSnapshot(spark, new TableIdentifier(tableName))._2.metadata

  private def protocolOf(tableName: String) =
    DeltaLog.forTableWithSnapshot(spark, new TableIdentifier(tableName))._2.protocol

  test("feature is a ReaderWriterFeature and a RemovableFeature") {
    assert(AdaptiveMetadataTableFeature.isReaderWriterFeature)
    assert(AdaptiveMetadataTableFeature.isInstanceOf[RemovableFeature])
  }

  test("feature is not automatically enabled by metadata") {
    assert(!AdaptiveMetadataTableFeature.isInstanceOf[FeatureAutomaticallyEnabledByMetadata])
  }

  test("feature requires catalogManaged, rowTracking, domainMetadata, DVs and column mapping") {
    assert(AdaptiveMetadataTableFeature.requiredFeatures === Set(
      CatalogOwnedTableFeature,
      RowTrackingFeature,
      DomainMetadataTableFeature,
      DeletionVectorsTableFeature,
      ColumnMappingTableFeature))
  }

  /** All features feature depends on, transitively. */
  private def dependentFeatures(feature: TableFeature): Seq[TableFeature] = {
    feature.requiredFeatures.toSeq ++ feature.requiredFeatures.toSeq.flatMap(dependentFeatures)
  }

  test("creating the table records it and all required features as supported in the protocol") {
    withTable("amt_supported") {
      createAdaptiveMetadataTable("amt_supported")
      val protocol = protocolOf("amt_supported")
      assert(protocol.isFeatureSupported(AdaptiveMetadataTableFeature),
        s"Protocol must record adaptiveMetadata-preview as supported. Got: $protocol")
      assert(AMTUtils.amtEnabled(metadataOf("amt_supported"), protocol),
        s"AMTUtils.amtEnabled must report the table as AMT-enabled. Got: $protocol")
      // Every required feature (and their transitive dependencies) must be present.
      dependentFeatures(AdaptiveMetadataTableFeature).foreach { f =>
        assert(protocol.readerAndWriterFeatureNames.contains(f.name),
          s"Protocol must contain required feature ${f.name}. Got: $protocol")
      }
    }
  }

  test("creating the table does not add incompatible features to the protocol") {
    withTable("amt_without_incompatible_features") {
        createAdaptiveMetadataTable("amt_without_incompatible_features")
        val protocol = protocolOf("amt_without_incompatible_features")
        assert(!protocol.isFeatureSupported(V2CheckpointTableFeature),
          s"Protocol must not support v2Checkpoint. Got: $protocol")
    }
  }

  /** Asserts that neither the protocol nor the metadata of `tableName` asks for v2 checkpoints. */
  private def assertNoV2Checkpoint(tableName: String): Unit = {
    val protocol = protocolOf(tableName)
    val metadata = metadataOf(tableName)
    assert(!protocol.isFeatureSupported(V2CheckpointTableFeature),
      s"Protocol must not support v2Checkpoint. Got: $protocol")
    assert(!DeltaConfigs.CHECKPOINT_POLICY.fromMetaData(metadata).needsV2CheckpointSupport,
      s"Metadata must not set checkpointPolicy=v2. Got: ${metadata.configuration}")
  }


  /** Asserts that `f` fails because it would put v2Checkpoint on the AMT table `tableName`. */
  private def assertV2CheckpointRejected(tableName: String)(f: => Unit): Unit = {
    checkError(
      intercept[DeltaUnsupportedOperationException](f),
      condition = "DELTA_INCOMPATIBLE_TABLE_FEATURES",
      parameters = Map(
        // The qualified name, or the table path when the transaction has no catalog table yet,
        // as on CREATE.
        "tableName" -> s".*[./]$tableName",
        "feature1" -> AdaptiveMetadataTableFeature.name,
        "feature2" -> V2CheckpointTableFeature.name),
      matchPVals = true)
  }

  test("creating the table with checkpointPolicy=v2 is rejected") {
    withTable("amt_create_v2_policy") {
      assertV2CheckpointRejected("amt_create_v2_policy") {
        createAdaptiveMetadataTable(
          "amt_create_v2_policy", Map(DeltaConfigs.CHECKPOINT_POLICY.key -> "v2"))
      }
    }
  }

  test("creating the table with a checkpointPolicy=v2 session default is rejected") {
    withTable("amt_create_v2_policy_default") {
      withSQLConf(DeltaConfigs.CHECKPOINT_POLICY.defaultTablePropertyKey -> "v2") {
        assertV2CheckpointRejected("amt_create_v2_policy_default") {
          createAdaptiveMetadataTable("amt_create_v2_policy_default")
        }
      }
    }
  }

  test("creating the table with the v2Checkpoint feature property is rejected") {
    withTable("amt_create_v2_feature") {
      assertV2CheckpointRejected("amt_create_v2_feature") {
        createAdaptiveMetadataTable(
          "amt_create_v2_feature",
          Map(propertyKey(V2CheckpointTableFeature) -> FEATURE_PROP_SUPPORTED))
      }
    }
  }

  test("setting checkpointPolicy=v2 on the table is rejected") {
    withTable("amt_alter_v2_policy") {
      createAdaptiveMetadataTable("amt_alter_v2_policy")
      assertV2CheckpointRejected("amt_alter_v2_policy") {
        sql(s"ALTER TABLE amt_alter_v2_policy SET TBLPROPERTIES " +
          s"('${DeltaConfigs.CHECKPOINT_POLICY.key}' = 'v2')")
      }
      assertNoV2Checkpoint("amt_alter_v2_policy")
    }
  }

  test("adding the v2Checkpoint feature property on the table is rejected") {
    withTable("amt_alter_v2_feature") {
      createAdaptiveMetadataTable("amt_alter_v2_feature")
      assertV2CheckpointRejected("amt_alter_v2_feature") {
        sql(s"ALTER TABLE amt_alter_v2_feature SET TBLPROPERTIES " +
          s"('${propertyKey(V2CheckpointTableFeature)}' = '$FEATURE_PROP_SUPPORTED')")
      }
      assertNoV2Checkpoint("amt_alter_v2_feature")
    }
  }

  test("replacing the table with checkpointPolicy=v2 is rejected") {
    withTable("amt_replace_v2_policy") {
      createAdaptiveMetadataTable("amt_replace_v2_policy")
      assertV2CheckpointRejected("amt_replace_v2_policy") {
        createAdaptiveMetadataTable(
          "amt_replace_v2_policy",
          Map(DeltaConfigs.CHECKPOINT_POLICY.key -> "v2"),
          command = "CREATE OR REPLACE TABLE")
      }
      assertNoV2Checkpoint("amt_replace_v2_policy")
    }
  }

  test("replacing a table that has v2Checkpoint with an AMT table is rejected") {
    withTable("v2_checkpoint_replace_amt") {
      // REPLACE cannot make a table catalog-managed or change its column mapping mode, both of
      // which AMT needs, so start from such a table.
      sql(s"CREATE TABLE v2_checkpoint_replace_amt (id INT) USING $tableProvider " +
        s"TBLPROPERTIES ('${propertyKey(CatalogOwnedTableFeature)}' = '$FEATURE_PROP_SUPPORTED', " +
        s"'${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'id', " +
        s"'${DeltaConfigs.CHECKPOINT_POLICY.key}' = 'v2')")
      assert(protocolOf("v2_checkpoint_replace_amt").isFeatureSupported(V2CheckpointTableFeature))

      // REPLACE keeps the protocol, and with it v2Checkpoint, so the AMT table is rejected.
      assertV2CheckpointRejected("v2_checkpoint_replace_amt") {
        createAdaptiveMetadataTable(
          "v2_checkpoint_replace_amt", command = "CREATE OR REPLACE TABLE")
      }
      assert(!protocolOf("v2_checkpoint_replace_amt")
        .isFeatureSupported(AdaptiveMetadataTableFeature))

      // When REPLACE may reset the protocol, v2Checkpoint is dropped and the AMT table is created.
      withSQLConf(DeltaSQLConf.REPLACE_TABLE_PROTOCOL_DOWNGRADE_ALLOWED.key -> "true") {
        createAdaptiveMetadataTable(
          "v2_checkpoint_replace_amt", command = "CREATE OR REPLACE TABLE")
      }
      assert(protocolOf("v2_checkpoint_replace_amt")
        .isFeatureSupported(AdaptiveMetadataTableFeature))
      assertNoV2Checkpoint("v2_checkpoint_replace_amt")
    }
  }

  test("a commit that adds v2Checkpoint through a protocol action is rejected") {
    withTable("amt_protocol_action_v2") {
      createAdaptiveMetadataTable("amt_protocol_action_v2")
      val catalogTable =
        spark.sessionState.catalog.getTableMetadata(TableIdentifier("amt_protocol_action_v2"))
      val (deltaLog, snapshot) =
        DeltaLog.forTableWithSnapshot(spark, TableIdentifier("amt_protocol_action_v2"))
      // A protocol action bypasses updateMetadata, so only the check on the final protocol at
      // commit time can catch it.
      val protocolWithV2 = snapshot.protocol.withFeature(V2CheckpointTableFeature)
      assertV2CheckpointRejected("amt_protocol_action_v2") {
        deltaLog.startTransaction(Some(catalogTable), Some(snapshot))
          .commit(Seq(protocolWithV2), DeltaOperations.UpgradeProtocol(protocolWithV2))
      }
      assertNoV2Checkpoint("amt_protocol_action_v2")
    }
  }

  test("the feature property is not persisted in the table metadata configuration") {
    withTable("amt_no_feature_prop") {
      createAdaptiveMetadataTable("amt_no_feature_prop")
      // AdaptiveMetadataTableFeature does not have [[metadataRequiresFeatureToBeEnabled]], so no
      // metadata property drives its enablement.
      val metadata = metadataOf("amt_no_feature_prop")
      val featurePropKey = propertyKey(AdaptiveMetadataTableFeature)
      assert(!metadata.configuration.contains(featurePropKey),
        s"'$featurePropKey' should not be present in metadata configuration. " +
          s"Got: ${metadata.configuration}")
      // Sanity check: the feature is still recorded in the protocol.
      assert(protocolOf("amt_no_feature_prop").isFeatureSupported(AdaptiveMetadataTableFeature))
    }
  }

  test("creating the table auto-enables rowTracking, deletionVectors and id column mapping") {
    withTable("amt_enable") {
      createAdaptiveMetadataTable("amt_enable")
      val metadata = metadataOf("amt_enable")
      assert(metadata.columnMappingMode.name === "id",
        s"Column mapping mode should be auto-set to id. Got: ${metadata.columnMappingMode}")
      assert(DeltaConfigs.ROW_TRACKING_ENABLED.fromMetaData(metadata),
        "rowTracking should be enabled in metadata.")
      assert(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.fromMetaData(metadata),
        "deletionVectors should be enabled in metadata.")
      // ICT is required by catalogManaged and must be enabled as well.
      assert(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(metadata),
        "In-commit timestamps should be enabled in metadata.")
    }
  }

  test("creating the table with explicit id column mapping mode succeeds") {
    withTable("amt_explicit_id") {
      createAdaptiveMetadataTable(
        "amt_explicit_id", Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> "id"))
      assert(metadataOf("amt_explicit_id").columnMappingMode.name === "id")
    }
  }

  test("creating the table with non-id column mapping mode is rejected") {
    withTable("amt_bad_mode") {
      val ex = intercept[DeltaAnalysisException] {
        createAdaptiveMetadataTable(
          "amt_bad_mode", Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> "name"))
      }
      assert(ex.getErrorClass === "DELTA_ADAPTIVE_METADATA_REQUIRES_COLUMN_MAPPING_ID_MODE",
        s"Unexpected error: ${ex.getMessage}")
    }
  }

  for (prop <- Seq(
      DeltaConfigs.ROW_TRACKING_ENABLED.key,
      DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key)) {
    test(s"creating the table with $prop explicitly disabled is rejected") {
      withTable("amt_dep_disabled") {
        val ex = intercept[DeltaAnalysisException] {
          createAdaptiveMetadataTable("amt_dep_disabled", Map(prop -> "false"))
        }
        assert(
          ex.getErrorClass === "DELTA_ADAPTIVE_METADATA_REQUIRES_DEPENDENT_FEATURE_ENABLED",
          s"Unexpected error: ${ex.getMessage}")
      }
    }
  }

  test("enabling the feature on an existing table is rejected") {
    withTable("amt_upgrade") {
      sql(s"CREATE TABLE amt_upgrade (id INT) USING $tableProvider")
      val ex = intercept[DeltaAnalysisException] {
        sql(s"ALTER TABLE amt_upgrade SET TBLPROPERTIES " +
          s"('${propertyKey(AdaptiveMetadataTableFeature)}' = '$FEATURE_PROP_SUPPORTED')")
      }
      assert(ex.getErrorClass === "DELTA_ADAPTIVE_METADATA_UPGRADE_NOT_SUPPORTED",
        s"Unexpected error: ${ex.getMessage}")
    }
  }

  test("enabling the feature is rejected when the kill-switch SQL conf is false") {
    withSQLConf(DeltaSQLConf.V4_ADAPTIVE_METADATA_TABLE_PREVIEW_ENABLED.key -> "false") {
      // The feature is unregistered, so featureNameToFeature returns None.
      assert(TableFeature.featureNameToFeature(AdaptiveMetadataTableFeature.name).isEmpty)

      withTable("amt_killswitch") {
        val ex = intercept[DeltaTableFeatureException] {
          createAdaptiveMetadataTable("amt_killswitch")
        }
        assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("unsupported"),
          s"Expected an unsupported-feature error, got: ${ex.getMessage}")
      }
    }
  }
}
