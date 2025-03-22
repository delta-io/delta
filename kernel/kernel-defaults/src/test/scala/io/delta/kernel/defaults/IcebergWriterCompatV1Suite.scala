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
package io.delta.kernel.defaults

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel.{Operation, Table}
import io.delta.kernel.data.Row
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.TableConfig
import io.delta.kernel.internal.icebergcompat.IcebergCompatV2MetadataValidatorAndUpdaterSuiteBase.COMPLEX_TYPES
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.internal.util.{ColumnMappingSuiteBase, VectorUtils}
import io.delta.kernel.internal.util.ColumnMapping.ColumnMappingMode
import io.delta.kernel.types.{ByteType, DataType, FieldMetadata, IntegerType, ShortType, StringType, StructType, TimestampNTZType, TimestampType, VariantType}
import io.delta.kernel.utils.CloseableIterable.emptyIterable

class IcebergWriterCompatV1Suite extends DeltaTableWriteSuiteBase with ColumnMappingSuiteBase {

  private val tblPropertiesIcebergWriterCompatV1Enabled = Map(
    TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.getKey -> "true")

  private val tblPropertiesIcebergCompatV2Enabled = Map(
    TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")

  private val tblPropertiesColumnMappingModeId = Map(
    TableConfig.COLUMN_MAPPING_MODE.getKey -> "id")

  def verifyIcebergWriterCompatV1Enabled(tablePath: String, engine: Engine): Unit = {
    val protocol = getProtocol(engine, tablePath)
    val metadata = getMetadata(engine, tablePath)

    // Check expected protocol features are enabled
    assert(protocol.supportsFeature(TableFeatures.ICEBERG_COMPAT_V2_W_FEATURE))
    assert(protocol.supportsFeature(TableFeatures.COLUMN_MAPPING_RW_FEATURE))
    assert(protocol.supportsFeature(TableFeatures.ICEBERG_WRITER_COMPAT_V1))

    // Check expected confs are present
    assert(TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.fromMetadata(metadata))
    assert(TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(metadata))
    assert(TableConfig.COLUMN_MAPPING_MODE.fromMetadata(metadata) == ColumnMappingMode.ID)
  }

  Seq(
    (Map(), "no other properties"),
    (tblPropertiesIcebergCompatV2Enabled, "icebergCompatV2 enabled"),
    (tblPropertiesColumnMappingModeId, "column mapping mode set to id"),
    (
      tblPropertiesIcebergCompatV2Enabled ++ tblPropertiesColumnMappingModeId,
      "icebergCompatV2 enabled and column mapping mode set to id")).foreach {
    case (tblProperties, description) =>
      test(s"Basic enablement on new table with $description") {
        withTempDirAndEngine { (tablePath, engine) =>
          createEmptyTable(
            engine,
            tablePath,
            cmTestSchema(),
            tableProperties = tblPropertiesIcebergWriterCompatV1Enabled ++ tblProperties)
          verifyIcebergWriterCompatV1Enabled(tablePath, engine)
          verifyCMTestSchemaHasValidColumnMappingInfo(getMetadata(engine, tablePath))
        }
      }
  }

  test("Cannot enable on an existing table") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath,
        testSchema,
        tableProperties = tblPropertiesColumnMappingModeId ++ tblPropertiesIcebergCompatV2Enabled)
      val e = intercept[KernelException] {
        updateTableMetadata(
          engine,
          tablePath,
          tableProperties = tblPropertiesIcebergWriterCompatV1Enabled)
      }
      assert(e.getMessage.contains(
        "Cannot enable delta.enableIcebergWriterCompatV1 on an existing table"))
    }
  }

  test("Can enable on an existing table if already enabled") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath,
        testSchema,
        tableProperties = tblPropertiesIcebergWriterCompatV1Enabled)
      verifyIcebergWriterCompatV1Enabled(tablePath, engine)
      updateTableMetadata(
        engine,
        tablePath,
        tableProperties = tblPropertiesIcebergWriterCompatV1Enabled)
      verifyIcebergWriterCompatV1Enabled(tablePath, engine)
    }
  }

  test("Cannot enable when column mapping mode explicitly set to name/none") {
    Seq("name", "none").foreach { cmMode =>
      withTempDirAndEngine { (tablePath, engine) =>
        val e = intercept[KernelException] {
          createEmptyTable(
            engine,
            tablePath,
            testSchema,
            tableProperties = tblPropertiesIcebergWriterCompatV1Enabled ++
              Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> cmMode))
        }
        assert(e.getMessage.contains(s"The value '$cmMode' for the property " +
          s"'delta.columnMapping.mode' is not compatible with icebergWriterCompatV1"))
      }
    }
  }

  test("Cannot enable when icebergCompatV2 explicitly disabled") {
    withTempDirAndEngine { (tablePath, engine) =>
      val e = intercept[KernelException] {
        createEmptyTable(
          engine,
          tablePath,
          testSchema,
          tableProperties = tblPropertiesIcebergWriterCompatV1Enabled ++
            Map(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "false"))
      }
      assert(e.getMessage.contains("'false' for the property 'delta.enableIcebergCompatV2' is " +
        "not compatible with icebergWriterCompatV1"))
    }
  }

  test("Can disable icebergWriterCompatV1 conf and checks are skipped") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create an empty table with icebergWriterCompatV1 enabled
      createEmptyTable(
        engine,
        tablePath,
        cmTestSchema(),
        tableProperties = tblPropertiesIcebergWriterCompatV1Enabled)
      verifyIcebergWriterCompatV1Enabled(tablePath, engine)

      // Disable icebergWriterCompatV1 in the table properties
      updateTableMetadata(
        engine,
        tablePath,
        tableProperties = Map(TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.getKey -> "false"))
      assert(
        !TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.fromMetadata(getMetadata(engine, tablePath)))

      // Disable icebergCompatV2 (disallowed by the checks for icebergWriterCompatV1)
      updateTableMetadata(
        engine,
        tablePath,
        tableProperties = Map(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "false"))
      assert(!TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(getMetadata(engine, tablePath)))
    }
  }

  // TODO once we support schema evolution test adding columns of these types
  Seq(ByteType.BYTE, ShortType.SHORT).foreach { dataType =>
    test(s"Cannot enable IcebergWriterCompatV2 on a table with datatype $dataType") {
      withTempDirAndEngine { (tablePath, engine) =>
        val e = intercept[KernelException] {
          createEmptyTable(
            engine,
            tablePath,
            new StructType().add("col", dataType),
            tableProperties = tblPropertiesIcebergWriterCompatV1Enabled)
        }
        assert(e.getMessage.contains(
          s"icebergWriterCompatV1 does not support the data types: [${dataType.toString}]"))
      }
    }
  }

  test("subsequent writes to icebergWriterCompatV1 enabled tables doesn't update metadata") {
    // we want to make sure the [[IcebergWriterCompatV1MetadataValidatorAndUpdater]] doesn't
    // make unneeded metadata updates
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath,
        testSchema,
        tableProperties = tblPropertiesIcebergWriterCompatV1Enabled)

      appendData(
        engine,
        tablePath,
        data = Seq.empty,
        tableProperties = tblPropertiesIcebergWriterCompatV1Enabled ++
          tblPropertiesIcebergCompatV2Enabled ++ tblPropertiesColumnMappingModeId
      ) // version 1
      appendData(engine, tablePath, data = Seq.empty) // version 2

      val table = Table.forPath(engine, tablePath)
      assert(getMetadataActionFromCommit(engine, table, version = 0).isDefined)
      assert(getMetadataActionFromCommit(engine, table, version = 1).isEmpty)
      assert(getMetadataActionFromCommit(engine, table, version = 2).isEmpty)

      // make a metadata update and see it is reflected in the table
      val newProps = Map("key" -> "value")
      updateTableMetadata(engine, tablePath, tableProperties = newProps) // version 3
      assert(getMetadataActionFromCommit(engine, table, version = 3).isDefined)
      val ver3Metadata = getMetadata(engine, tablePath)
      assert(ver3Metadata.getConfiguration().get("key") == "value")
    }
  }

  /* -------------------- Tests for blocked table features -------------------- */

  def testIncompatibleTableFeature(
      featureName: String,
      tablePropertiesToEnable: Map[String, String] = Map.empty,
      schemaToEnable: StructType = testSchema,
      expectedErrorMessage: String,
      testOnExistingTable: Boolean = true // some features cannot be enabled for existing tables
  ): Unit = {
    if (testOnExistingTable) {
      test(s"Cannot enable feature $featureName on an existing table with " +
        s"icebergWriterCompatV1 enabled") {
        withTempDirAndEngine { (tablePath, engine) =>
          // Create existing table with icebergWriterCompatV1 enabled
          createEmptyTable(
            engine,
            tablePath,
            testSchema,
            tableProperties = tblPropertiesIcebergWriterCompatV1Enabled)
          verifyIcebergWriterCompatV1Enabled(tablePath, engine)
          val e = intercept[KernelException] {
            // Update the table such that we enable the incompatible feature
            updateTableMetadata(
              engine,
              tablePath,
              schema = schemaToEnable,
              tableProperties = tablePropertiesToEnable)
          }
          assert(e.getMessage.contains(expectedErrorMessage))
        }
      }
    }
    test(s"Cannot enable feature $featureName and icebergWriterCompatV1 on a new table") {
      withTempDirAndEngine { (tablePath, engine) =>
        // Create table with IcebergCompatWriterV1 and the incompatible feature enabled
        val e = intercept[KernelException] {
          createEmptyTable(
            engine,
            tablePath,
            schema = schemaToEnable,
            tableProperties =
              tblPropertiesIcebergWriterCompatV1Enabled ++ tablePropertiesToEnable)
        }
        assert(e.getMessage.contains(expectedErrorMessage))
      }
    }
    // Since we don't support enabling icebergWriterCompatV1 on an existing table we cannot test
    // the case of enabling icebergWriterCompatV1 on an existing table with the incompatible
    // feature enabled
  }

  // Features that don't have write support currently (once we add write support convert these
  // tests and update error intercepted)
  def testIncompatibleUnsupportedTableFeature(
      featureName: String,
      tablePropertiesToEnable: Map[String, String] = Map.empty,
      schemaToEnable: StructType = testSchema,
      expectedErrorMessage: String = "Unsupported Delta writer feature",
      testOnExistingTable: Boolean = true // some features cannot be enabled for existing tables
  ): Unit = {
    testIncompatibleTableFeature(
      featureName,
      tablePropertiesToEnable,
      schemaToEnable,
      expectedErrorMessage,
      testOnExistingTable)
  }

  testIncompatibleUnsupportedTableFeature(
    "changeDataFeed",
    tablePropertiesToEnable = Map(TableConfig.CHANGE_DATA_FEED_ENABLED.getKey -> "true"))

  testIncompatibleUnsupportedTableFeature(
    "invariants",
    schemaToEnable = new StructType()
      .add("c1", IntegerType.INTEGER)
      .add(
        "c2",
        IntegerType.INTEGER,
        FieldMetadata.builder()
          .putString("delta.invariants", "{\"expression\": { \"expression\": \"x > 3\"} }")
          .build()),
    testOnExistingTable = false // we don't currently support schema updates
  )

  testIncompatibleUnsupportedTableFeature(
    "checkConstraints",
    tablePropertiesToEnable = Map("delta.constraints.a" -> "a = b"),
    expectedErrorMessage = "Unknown configuration was specified: delta.constraints.a")

  testIncompatibleUnsupportedTableFeature(
    "generatedColumns",
    schemaToEnable = new StructType()
      .add("c1", IntegerType.INTEGER)
      .add(
        "c2",
        IntegerType.INTEGER,
        FieldMetadata.builder()
          .putString("delta.generationExpression", "{\"expression\": \"c1 + 1\"}")
          .build()),
    testOnExistingTable = false // we don't currently support schema updates
  )

  testIncompatibleUnsupportedTableFeature(
    "identityColumns",
    schemaToEnable = new StructType()
      .add("c1", IntegerType.INTEGER)
      .add(
        "c2",
        IntegerType.INTEGER,
        FieldMetadata.builder()
          .putLong("delta.identity.start", 1L)
          .putLong("delta.identity.step", 2L)
          .putBoolean("delta.identity.allowExplicitInsert", true)
          .build()),
    testOnExistingTable = false // we don't currently support schema updates
  )

  testIncompatibleUnsupportedTableFeature(
    "variantType",
    schemaToEnable = new StructType()
      .add("c1", IntegerType.INTEGER)
      .add("c2", VariantType.VARIANT),
    testOnExistingTable = false, // we don't currently support schema updates
    // We throw an error earlier for variant for some reason
    expectedErrorMessage = "Kernel doesn't support writing data of type: variant")

  // typeWidening is blocked transitively by icebergCompatV2; update this test if that check changes
  testIncompatibleUnsupportedTableFeature(
    "typeWidening",
    tablePropertiesToEnable = Map("delta.enableTypeWidening" -> "true"))

  // For some reason rowTracking throws an UnsupportedOperationException (due to partial support?)
  // so cannot use test fx here
  test(
    s"Cannot enable feature rowTracking on an existing table with icebergWriterCompatV1 enabled") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create existing table with icebergWriterCompatV1 enabled
      createEmptyTable(
        engine,
        tablePath,
        testSchema,
        tableProperties = tblPropertiesIcebergWriterCompatV1Enabled)
      verifyIcebergWriterCompatV1Enabled(tablePath, engine)
      val e = intercept[UnsupportedOperationException] {
        // Update the table such that we enable rowTracking
        updateTableMetadata(
          engine,
          tablePath,
          tableProperties = Map("delta.enableRowTracking" -> "true"))
      }
      assert(e.getMessage.contains("Feature `rowTracking` is not yet supported in Kernel"))
    }
  }

  test(s"Cannot enable feature rowTracking and icebergWriterCompatV1 on a new table") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create table with IcebergCompatWriterV1 and rowTracking enabled
      val e = intercept[UnsupportedOperationException] {
        createEmptyTable(
          engine,
          tablePath,
          testSchema,
          tableProperties =
            tblPropertiesIcebergWriterCompatV1Enabled ++ Map("delta.enableRowTracking" -> "true"))
      }
      assert(e.getMessage.contains("Feature `rowTracking` is not yet supported in Kernel"))
    }
  }

  // deletionVectors is blocked by both icebergCompatV2 and icebergWriterCompatV1; since the
  // icebergCompatV2 checks are executed first as part of ICEBERG_COMPAT_V2_ENABLED.postProcess we
  // hit that error message first
  testIncompatibleTableFeature(
    "deletionVectors",
    tablePropertiesToEnable = Map(TableConfig.DELETION_VECTORS_CREATION_ENABLED.getKey -> "true"),
    expectedErrorMessage =
      "Table features [deletionVectors] are incompatible with icebergCompatV2")

  test("All expected compatible features can be enabled with icebergWriterCompatV1") {

    val tblProperties = Map(
      TableConfig.APPEND_ONLY_ENABLED.getKey -> "true", // appendOnly
      TableConfig.CHECKPOINT_POLICY.getKey -> "v2", // checkpointV2
      TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true", // inCommitTimestamp
      TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.getKey -> "true")
    val schema = new StructType()
      .add("c1", IntegerType.INTEGER)
      .add("c2", TimestampNTZType.TIMESTAMP_NTZ) // timestampNtz

    // New table with these features + icebergWriterCompatV1
    withTempDirAndEngine { (tablePath, engine) =>
      Table.forPath(engine, tablePath)
        .createTransactionBuilder(engine, "engineInfo-test", Operation.WRITE)
        .withSchema(engine, schema)
        .withTableProperties(engine, tblProperties.asJava)
        .withDomainMetadataSupported() // domainMetadata
        .build(engine)
        .commit(engine, emptyIterable[Row])
      verifyIcebergWriterCompatV1Enabled(tablePath, engine)
      // Check all the features are supported
      val protocol = getProtocol(engine, tablePath)
      assert(protocol.supportsFeature(TableFeatures.APPEND_ONLY_W_FEATURE))
      assert(protocol.supportsFeature(TableFeatures.CHECKPOINT_V2_RW_FEATURE))
      assert(protocol.supportsFeature(TableFeatures.IN_COMMIT_TIMESTAMP_W_FEATURE))
      assert(protocol.supportsFeature(TableFeatures.TIMESTAMP_NTZ_RW_FEATURE))
      assert(protocol.supportsFeature(TableFeatures.DOMAIN_METADATA_W_FEATURE))
      assert(protocol.supportsFeature(TableFeatures.INVARIANTS_W_FEATURE))
      // TODO in the future add typeWidening and clustering once they are supported
    }

    // Existing table with icebergWriterCompatV1 - enable these features
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath,
        cmTestSchema(),
        tableProperties = tblPropertiesIcebergWriterCompatV1Enabled)
      verifyIcebergWriterCompatV1Enabled(tablePath, engine)

      Table.forPath(engine, tablePath)
        .createTransactionBuilder(engine, "engineInfo-test", Operation.WRITE)
        //  .withSchema(engine, schema) - we don't support schema updates currently
        .withTableProperties(engine, tblProperties.asJava)
        .withDomainMetadataSupported()
        .build(engine)
        .commit(engine, emptyIterable[Row])
      // Check all the features are supported
      val protocol = getProtocol(engine, tablePath)
      assert(protocol.supportsFeature(TableFeatures.APPEND_ONLY_W_FEATURE))
      assert(protocol.supportsFeature(TableFeatures.CHECKPOINT_V2_RW_FEATURE))
      assert(protocol.supportsFeature(TableFeatures.IN_COMMIT_TIMESTAMP_W_FEATURE))
      // assert(protocol.supportsFeature(TableFeatures.TIMESTAMP_NTZ_RW_FEATURE))
      assert(protocol.supportsFeature(TableFeatures.DOMAIN_METADATA_W_FEATURE))
      assert(protocol.supportsFeature(TableFeatures.INVARIANTS_W_FEATURE))
      // TODO in the future add typeWidening and clustering once they are supported
    }
  }

  /* -------------------- Enforcements blocked by icebergCompatV2 -------------------- */
  // We test the typeWidening and deletionVector checks above as part of blocked table feature tests

  // We cannot test enabling icebergCompatV1 since it is not a table feature in Kernel; This is
  // tested in the unit tests in IcebergWriterCompatV1MetadataValidatorAndUpdaterSuite

  (SIMPLE_TYPES ++ COMPLEX_TYPES)
    // filter out the types unsupported by icebergWriterCompatV1
    .filter(dataType => dataType != ByteType.BYTE && dataType != ShortType.SHORT)
    .foreach { dataType: DataType =>
      test(s"allowed data column types: $dataType on a new table") {
        withTempDirAndEngine { (tablePath, engine) =>
          val schema = new StructType().add("col", dataType)
          createEmptyTable(
            engine,
            tablePath,
            schema,
            tableProperties = tblPropertiesIcebergWriterCompatV1Enabled)
        }
      }
    }

  ignore("test unsupported data types") {
    // Can't test this now as the only unsupported data type in Iceberg is VariantType,
    // and it also has no write support in Kernel.
    // Unit test for this is covered in [[IcebergWriterCompatV1MetadataValidatorAndUpdaterSuite]]
  }
}
