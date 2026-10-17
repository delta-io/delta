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

import java.io.File

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel.Table
import io.delta.kernel.defaults.utils.{AbstractWriteUtils, TestRow, WriteUtils, WriteUtilsWithV2Builders}
import io.delta.kernel.exceptions.InvalidConfigurationValueException
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.{InternalScanFileUtils, ScanImpl, TableConfig}
import io.delta.kernel.internal.util.{ColumnMapping, ColumnMappingSuiteBase}
import io.delta.kernel.types.{ArrayType, FieldMetadata, IntegerType, MapType, StringType, StructField, StructType}

import org.apache.spark.sql.delta.DeltaLog

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.scalatest.funsuite.AnyFunSuite

class DeltaColumnMappingTransactionBuilderV1Suite extends DeltaColumnMappingSuiteBase
    with WriteUtils {}

class DeltaColumnMappingTransactionBuilderV2Suite extends DeltaColumnMappingSuiteBase
    with WriteUtilsWithV2Builders {}

trait DeltaColumnMappingSuiteBase extends AnyFunSuite with AbstractWriteUtils
    with ColumnMappingSuiteBase {

  val simpleTestSchema = new StructType()
    .add("a", StringType.STRING, true)
    .add("b", IntegerType.INTEGER, true)

  test("create table with unsupported column mapping mode") {
    withTempDirAndEngine { (tablePath, engine) =>
      val ex = intercept[InvalidConfigurationValueException] {
        val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "invalid")
        createEmptyTable(engine, tablePath, simpleTestSchema, tableProperties = props)
      }
      assert(ex.getMessage.contains("Invalid value for table property " +
        "'delta.columnMapping.mode': 'invalid'. Needs to be one of: [none, id, name]."))
    }
  }

  test("create table with column mapping mode = none") {
    withTempDirAndEngine { (tablePath, engine) =>
      val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "none")
      createEmptyTable(engine, tablePath, simpleTestSchema, tableProperties = props)

      assert(getMetadata(engine, tablePath).getSchema.equals(simpleTestSchema))
    }
  }

  test("cannot update table with unsupported column mapping mode") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath, simpleTestSchema)

      val ex = intercept[InvalidConfigurationValueException] {
        val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "invalid")
        updateTableMetadata(engine, tablePath, tableProperties = props)
      }
      assert(ex.getMessage.contains("Invalid value for table property " +
        "'delta.columnMapping.mode': 'invalid'. Needs to be one of: [none, id, name]."))
    }
  }

  test("new table with column mapping mode = name") {
    withTempDirAndEngine { (tablePath, engine) =>
      val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "name")
      createEmptyTable(engine, tablePath, simpleTestSchema, tableProperties = props)

      val structType = getMetadata(engine, tablePath).getSchema
      assertColumnMapping(structType.get("a"), 1)
      assertColumnMapping(structType.get("b"), 2)

      val protocol = getProtocol(engine, tablePath)
      assert(protocol.getMinReaderVersion == 2 && protocol.getMinWriterVersion == 7)
    }
  }

  test("new table with column mapping mode = id") {
    withTempDirAndEngine { (tablePath, engine) =>
      val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "id")
      createEmptyTable(engine, tablePath, simpleTestSchema, tableProperties = props)

      val structType = getMetadata(engine, tablePath).getSchema
      assertColumnMapping(structType.get("a"), 1)
      assertColumnMapping(structType.get("b"), 2)

      assert(TableConfig.COLUMN_MAPPING_MAX_COLUMN_ID.fromMetadata(getMetadata(
        engine,
        tablePath)) == 2)

      val protocol = getProtocol(engine, tablePath)
      assert(protocol.getMinReaderVersion == 2 && protocol.getMinWriterVersion == 7)
    }
  }

  test("new table with existing column mappings in schema writes COLUMN_MAPPING_MAX_COLUMN_ID") {
    withTempDirAndEngine { (tablePath, engine) =>
      val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "id")
      val fieldMetadata = FieldMetadata.builder()
        .putLong(ColumnMapping.COLUMN_MAPPING_ID_KEY, 1)
        .putString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, "col-0").build()
      val structField = new StructField("col_name", IntegerType.INTEGER, false, fieldMetadata)
      val schema = new StructType(Seq(structField).asJava)
      createEmptyTable(engine, tablePath, schema, tableProperties = props)

      val structtype = getMetadata(engine, tablePath).getSchema
      assertColumnMapping(structtype.get("col_name"), 1)
      assert(TableConfig.COLUMN_MAPPING_MAX_COLUMN_ID.fromMetadata(getMetadata(
        engine,
        tablePath)) == 1)
    }
  }

  test("can update existing table to column mapping mode = name") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath, simpleTestSchema)
      val structType = getMetadata(engine, tablePath).getSchema
      assert(structType.equals(simpleTestSchema))

      val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "name")
      updateTableMetadata(engine, tablePath, tableProperties = props)

      val updatedSchema = getMetadata(engine, tablePath).getSchema
      assertColumnMapping(updatedSchema.get("a"), 1, "a")
      assertColumnMapping(updatedSchema.get("b"), 2, "b")
    }
  }

  Seq("name", "id").foreach { startingCMMode =>
    test(s"cannot update table with unsupported column mapping mode change: $startingCMMode") {
      withTempDirAndEngine { (tablePath, engine) =>
        val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> startingCMMode)
        createEmptyTable(engine, tablePath, simpleTestSchema, tableProperties = props)

        val structType = getMetadata(engine, tablePath).getSchema
        assertColumnMapping(structType.get("a"), 1)
        assertColumnMapping(structType.get("b"), 2)

        val ex = intercept[IllegalArgumentException] {
          val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "none")
          updateTableMetadata(engine, tablePath, tableProperties = props)
        }
        assert(ex.getMessage.contains(s"Changing column mapping mode " +
          s"from '$startingCMMode' to 'none' is not supported"))
      }
    }
  }

  test("cannot update column mapping mode from name to id on existing table") {
    withTempDirAndEngine { (tablePath, engine) =>
      val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "name")
      createEmptyTable(engine, tablePath, simpleTestSchema, tableProperties = props)

      val structType = getMetadata(engine, tablePath).getSchema
      assertColumnMapping(structType.get("a"), 1)
      assertColumnMapping(structType.get("b"), 2)

      val ex = intercept[IllegalArgumentException] {
        val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "id")
        updateTableMetadata(engine, tablePath, tableProperties = props)
      }
      assert(ex.getMessage.contains("Changing column mapping mode " +
        "from 'name' to 'id' is not supported"))
    }
  }

  test("cannot update column mapping mode from none to id on existing table") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath, simpleTestSchema)

      val structType = getMetadata(engine, tablePath).getSchema
      assert(structType.equals(simpleTestSchema))

      val ex = intercept[IllegalArgumentException] {
        val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "id")
        updateTableMetadata(engine, tablePath, tableProperties = props)
      }
      assert(ex.getMessage.contains("Changing column mapping mode " +
        "from 'none' to 'id' is not supported"))
    }
  }

  test("update table properties on a column mapping enabled table") {
    withTempDirAndEngine { (tablePath, engine) =>
      val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "name")
      createEmptyTable(engine, tablePath, simpleTestSchema, tableProperties = props)

      val metadata = getMetadata(engine, tablePath)
      assertColumnMapping(metadata.getSchema.get("a"), 1)
      assertColumnMapping(metadata.getSchema.get("b"), 2)

      val newProps = Map("key" -> "value")
      updateTableMetadata(engine, tablePath, tableProperties = newProps)

      assert(getMetadata(engine, tablePath).getConfiguration.get("key") == "value")
    }
  }

  Seq(true, false).foreach { withIcebergCompatV2 =>
    test(s"new table with column mapping mode = name and nested schema, " +
      s"enableIcebergCompatV2 = $withIcebergCompatV2") {
      withTempDirAndEngine { (tablePath, engine) =>
        val props = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "name",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> withIcebergCompatV2.toString)

        createEmptyTable(engine, tablePath, cmTestSchema(), tableProperties = props)

        verifyCMTestSchemaHasValidColumnMappingInfo(
          getMetadata(engine, tablePath),
          isNewTable = true,
          enableIcebergCompatV2 = withIcebergCompatV2)
      }
    }
  }

  test("subsequent updates don't update the metadata again when there is no change") {
    withTempDirAndEngine { (tablePath, engine) =>
      val props = Map(
        TableConfig.COLUMN_MAPPING_MODE.getKey -> "name",
        TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")

      createEmptyTable(engine, tablePath, testSchema, tableProperties = props)

      appendData(engine, tablePath, data = Seq.empty) // version 1
      appendData(engine, tablePath, data = Seq.empty) // version 2

      val table = Table.forPath(engine, tablePath)
      assert(getMetadataActionFromCommit(engine, table, version = 0).isDefined)
      assert(getMetadataActionFromCommit(engine, table, version = 1).isEmpty)
      assert(getMetadataActionFromCommit(engine, table, version = 2).isEmpty)
    }
  }

  // ===========================================================================
  // Data write tests
  // ===========================================================================

  Seq("name", "id").foreach { cmMode =>
    test(s"write data into unpartitioned column mapping table (mode=$cmMode) and read back") {
      withTempDirAndEngine { (tablePath, engine) =>
        val schema = new StructType()
          .add("id", IntegerType.INTEGER)
          .add("name", StringType.STRING)
        val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> cmMode)
        val data = generateData(schema, Seq.empty, Map.empty, batchSize = 50, numBatches = 2)

        appendData(
          engine,
          tablePath,
          isNewTable = true,
          schema = schema,
          data = Seq(Map.empty[String, Literal] -> data),
          tableProperties = props)

        // Read back via Kernel and verify logical data is intact
        checkTable(tablePath, data.flatMap(_.toTestRows), engine = engine)

        // Verify Parquet files are written under physical names (col-<uuid> for new tables)
        val committedSchema = getMetadata(engine, tablePath).getSchema
        val physicalNameId = ColumnMapping.getPhysicalName(committedSchema.get("id"))
        val physicalNameName = ColumnMapping.getPhysicalName(committedSchema.get("name"))
        assert(
          physicalNameId.startsWith("col-"),
          s"Expected physical name to start with 'col-' for id column, got: $physicalNameId")
        assert(
          physicalNameName.startsWith("col-"),
          s"Expected physical name to start with 'col-' for name column, got: $physicalNameName")

        val parquetFiles = new File(tablePath).listFiles()
          .filter(_.getName.endsWith(".parquet"))
        assert(parquetFiles.nonEmpty, "Expected at least one Parquet data file")
      }
    }
  }

  Seq("name", "id").foreach { cmMode =>
    test(s"write data into column mapping table (mode=$cmMode) is readable by Spark") {
      withTempDirAndEngine { (tablePath, engine) =>
        val schema = new StructType()
          .add("id", IntegerType.INTEGER)
          .add("name", StringType.STRING)
        val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> cmMode)
        val data = generateData(schema, Seq.empty, Map.empty, batchSize = 50, numBatches = 2)

        appendData(
          engine,
          tablePath,
          isNewTable = true,
          schema = schema,
          data = Seq(Map.empty[String, Literal] -> data),
          tableProperties = props)

        val expectedData = data.flatMap(_.toTestRows)

        // Kernel reads back the logical data correctly
        checkTable(tablePath, expectedData, engine = engine)

        // Confirm Spark can also read back the data
        val sparkDf = spark.read.format("delta").load(tablePath)
        assert(sparkDf.schema.fieldNames.toSeq == Seq("id", "name"))
        checkAnswer(sparkDf.collect().map(TestRow(_)).toSeq, expectedData)
      }
    }
  }

  Seq("name", "id").foreach { cmMode =>
    test(s"write data into partitioned column mapping table (mode=$cmMode) - " +
      s"partition dirs and AddFile use physical names") {
      withTempDirAndEngine { (tablePath, engine) =>
        val schema = new StructType()
          .add("value", IntegerType.INTEGER)
          .add("part", IntegerType.INTEGER)
        val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> cmMode)
        val partitionValues = Map("part" -> Literal.ofInt(42))
        val data = generateData(
          schema,
          Seq("part"),
          partitionValues,
          batchSize = 30,
          numBatches = 2)

        appendData(
          engine,
          tablePath,
          isNewTable = true,
          schema = schema,
          partCols = Seq("part"),
          data = Seq(partitionValues -> data),
          tableProperties = props)

        val committedSchema = getMetadata(engine, tablePath).getSchema
        val physicalPartName = ColumnMapping.getPhysicalName(committedSchema.get("part"))
        assert(
          physicalPartName.startsWith("col-"),
          s"Expected physical partition col name to start with 'col-', got: $physicalPartName")

        // The partition directory on disk must use the physical column name
        val partDir = new File(tablePath, s"$physicalPartName=42")
        assert(
          partDir.exists() && partDir.isDirectory,
          s"Expected partition directory '$physicalPartName=42' to exist at $tablePath")

        // AddFile.partitionValues keys must be physical names
        val snapshot = Table.forPath(engine, tablePath).getLatestSnapshot(engine)
        val scanFiles = snapshot.getScanBuilder().build()
          .asInstanceOf[ScanImpl]
          .getScanFiles(engine, true)
          .toSeq
          .flatMap(_.getRows.toSeq)
        assert(scanFiles.nonEmpty)
        scanFiles.foreach { row =>
          val partValues = InternalScanFileUtils.getPartitionValues(row).asScala
          assert(
            partValues.contains(physicalPartName),
            s"Expected AddFile.partitionValues to be keyed by physical name '$physicalPartName'," +
              s" got: ${partValues.keys.mkString(", ")}")
        }

        // Cross-engine check: Spark DeltaLog should also see physical partition names
        val addFiles = DeltaLog.forTable(spark, tablePath).update().allFiles.collect()
        assert(addFiles.nonEmpty)
        addFiles.foreach { addFile =>
          assert(
            addFile.partitionValues.contains(physicalPartName),
            s"Expected AddFile.partitionValues to be keyed by physical name '$physicalPartName'," +
              s" got: ${addFile.partitionValues.keys.mkString(", ")}")
        }

        // Logical data round-trips correctly
        checkTable(tablePath, data.flatMap(_.toTestRows), engine = engine)
      }
    }
  }

  Seq("name", "id").foreach { cmMode =>
    test(s"write multiple appends into column mapping table (mode=$cmMode) and read all back") {
      withTempDirAndEngine { (tablePath, engine) =>
        val schema = new StructType()
          .add("id", IntegerType.INTEGER)
        val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> cmMode)
        val data1 = generateData(schema, Seq.empty, Map.empty, batchSize = 100, numBatches = 2)
        val data2 = generateData(schema, Seq.empty, Map.empty, batchSize = 75, numBatches = 3)

        appendData(
          engine,
          tablePath,
          isNewTable = true,
          schema = schema,
          data = Seq(Map.empty[String, Literal] -> data1),
          tableProperties = props)

        appendData(
          engine,
          tablePath,
          data = Seq(Map.empty[String, Literal] -> data2))

        val expectedData = (data1 ++ data2).flatMap(_.toTestRows)
        checkTable(tablePath, expectedData, engine = engine)
      }
    }
  }

  test("upgrade existing table none->name then write data, old and new files readable") {
    withTempDirAndEngine { (tablePath, engine) =>
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("name", StringType.STRING)

      // Write data before enabling column mapping (physical names = logical names)
      val dataBefore = generateData(schema, Seq.empty, Map.empty, batchSize = 20, numBatches = 1)
      appendData(
        engine,
        tablePath,
        isNewTable = true,
        schema = schema,
        data = Seq(Map.empty[String, Literal] -> dataBefore))

      // Upgrade to name mode
      updateTableMetadata(
        engine,
        tablePath,
        tableProperties = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "name"))

      // Physical names after upgrade must reuse logical names (no rewrite of old files)
      val upgradedSchema = getMetadata(engine, tablePath).getSchema
      assert(ColumnMapping.getPhysicalName(upgradedSchema.get("id")) == "id")
      assert(ColumnMapping.getPhysicalName(upgradedSchema.get("name")) == "name")

      // Write more data after upgrade
      val dataAfter = generateData(schema, Seq.empty, Map.empty, batchSize = 20, numBatches = 1)
      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> dataAfter))

      // All rows from before and after upgrade must be readable
      val expectedData = (dataBefore ++ dataAfter).flatMap(_.toTestRows)
      checkTable(tablePath, expectedData, engine = engine)
    }
  }

  Seq("name", "id").foreach { cmMode =>
    test(s"write, rename a column, then write again (mode=$cmMode) - " +
      "old and new data readable by Kernel and Spark") {
      withTempDirAndEngine { (tablePath, engine) =>
        val schema = new StructType()
          .add("id", IntegerType.INTEGER)
          .add("name", StringType.STRING)
        val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> cmMode)

        // Write initial data under the original column names
        val dataBefore = generateData(schema, Seq.empty, Map.empty, batchSize = 20, numBatches = 1)
        appendData(
          engine,
          tablePath,
          isNewTable = true,
          schema = schema,
          data = Seq(Map.empty[String, Literal] -> dataBefore),
          tableProperties = props)

        // Rename "name" -> "full_name"
        val currentSchema = getMetadata(engine, tablePath).getSchema
        val renamedSchema = new StructType()
          .add("id", IntegerType.INTEGER, true, currentSchema.get("id").getMetadata)
          .add("full_name", StringType.STRING, true, currentSchema.get("name").getMetadata)
        updateTableMetadata(engine, tablePath, schema = renamedSchema)

        val updatedSchema = getMetadata(engine, tablePath).getSchema
        assert(updatedSchema.fieldNames().asScala.toSeq == Seq("id", "full_name"))
        // Physical name is preserved across the rename
        assert(
          ColumnMapping.getPhysicalName(updatedSchema.get("full_name")) ==
            ColumnMapping.getPhysicalName(currentSchema.get("name")))

        // Write more data after the rename
        val schemaAfterRename = new StructType()
          .add("id", IntegerType.INTEGER)
          .add("full_name", StringType.STRING)
        val dataAfter =
          generateData(schemaAfterRename, Seq.empty, Map.empty, batchSize = 20, numBatches = 1)
        appendData(
          engine,
          tablePath,
          data = Seq(Map.empty[String, Literal] -> dataAfter))

        // All rows -- written both before and after the rename -- must be readable under the
        // new logical column name via Kernel.
        val expectedData = (dataBefore ++ dataAfter).flatMap(_.toTestRows)
        checkTable(tablePath, expectedData, engine = engine)

        // Spark must also see the renamed logical schema and be able to read all the data,
        // including rows written before the column was renamed.
        val sparkDf = spark.read.format("delta").load(tablePath)
        assert(sparkDf.schema.fieldNames.toSeq == Seq("id", "full_name"))
        checkAnswer(sparkDf.collect().map(TestRow(_)).toSeq, expectedData)
      }
    }
  }

  test("id mode write: parquet.field.id is present in Parquet file footer for scalar columns") {
    withTempDirAndEngine { (tablePath, engine) =>
      val schema = new StructType()
        .add("x", IntegerType.INTEGER)
        .add("y", StringType.STRING)
      val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "id")
      val data = generateData(schema, Seq.empty, Map.empty, batchSize = 10, numBatches = 1)

      appendData(
        engine,
        tablePath,
        isNewTable = true,
        schema = schema,
        data = Seq(Map.empty[String, Literal] -> data),
        tableProperties = props)

      val parquetFiles = new File(tablePath).listFiles()
        .filter(_.getName.endsWith(".parquet"))
      assert(parquetFiles.nonEmpty)
      val footer = ParquetFileReader.readFooter(
        new Configuration(),
        new Path(parquetFiles.head.getAbsolutePath),
        ParquetMetadataConverter.NO_FILTER)
      footer.getFileMetaData.getSchema.getFields.asScala.foreach { field =>
        assert(
          field.getId != null,
          s"Expected parquet.field.id on column '${field.getName}' in id-mode table")
      }
    }
  }

  // nested struct column round-trips correctly
  Seq("name", "id").foreach { cmMode =>
    test(s"$cmMode mode write: table with nested struct column round-trips correctly") {
      withTempDirAndEngine { (tablePath, engine) =>
        val schema = new StructType()
          .add("id", IntegerType.INTEGER)
          .add(
            "nested",
            new StructType()
              .add("a", IntegerType.INTEGER)
              .add("b", StringType.STRING))
        val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> cmMode)
        val data = generateData(schema, Seq.empty, Map.empty, batchSize = 20, numBatches = 2)

        appendData(
          engine,
          tablePath,
          isNewTable = true,
          schema = schema,
          data = Seq(Map.empty[String, Literal] -> data),
          tableProperties = props)

        checkTable(tablePath, data.flatMap(_.toTestRows), engine = engine)

        val committedSchema = getMetadata(engine, tablePath).getSchema
        val nestedStruct = committedSchema.get("nested").getDataType.asInstanceOf[StructType]
        assert(ColumnMapping.getPhysicalName(committedSchema.get("nested")).startsWith("col-"))
        assert(ColumnMapping.getPhysicalName(nestedStruct.get("a")).startsWith("col-"))
        assert(ColumnMapping.getPhysicalName(nestedStruct.get("b")).startsWith("col-"))
      }
    }
  }
}
