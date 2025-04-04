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
package io.delta.kernel.internal.actions

import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.expressions.{Column, Literal}
import io.delta.kernel.internal.TableConfig
import io.delta.kernel.internal.data.TransactionStateRow
import io.delta.kernel.internal.util.{ColumnMapping, VectorUtils}
import io.delta.kernel.statistics.DataFileStatistics
import io.delta.kernel.types.{IntegerType, StringType, StructType}
import io.delta.kernel.utils.DataFileStatus

import org.scalatest.funsuite.AnyFunSuite

class GenerateIcebergCompatActionUtilsSuite extends AnyFunSuite {

  import GenerateIcebergCompatActionUtilsSuite._

  private def getTestTransactionStateRow(
      tblProperties: Map[String, String],
      maxRetries: Int = 0,
      partitionColumns: Seq[String] = Seq.empty): Row = {

    val metadata = new Metadata(
      "id",
      Optional.empty(), /* name */
      Optional.empty(), /* description */
      new Format(),
      testSchema.toJson,
      testSchema,
      VectorUtils.buildArrayValue(partitionColumns.asJava, StringType.STRING),
      Optional.empty(), /* createdTime */
      VectorUtils.stringStringMapValue(tblProperties.asJava))

    TransactionStateRow.of(
      ColumnMapping.updateColumnMappingMetadataIfNeeded(metadata, true).orElse(metadata),
      testTablePath,
      maxRetries)
  }

  /* ----- Error cases ----- */

  private def testErrorAddAndRemove(
      txnStateRow: Row,
      dataFileStatus: DataFileStatus,
      partitionValues: java.util.Map[String, Literal],
      dataChange: Boolean,
      expectedErrorMessageContains: String): Unit = {
    assert(
      intercept[UnsupportedOperationException] {
        GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1AddAction(
          txnStateRow,
          dataFileStatus,
          partitionValues,
          dataChange)
      }.getMessage.contains(expectedErrorMessageContains))
    assert(
      intercept[UnsupportedOperationException] {
        GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1RemoveAction(
          txnStateRow,
          dataFileStatus,
          partitionValues,
          dataChange)
      }.getMessage.contains(expectedErrorMessageContains))
  }

  test("GenerateIcebergCompatActionUtils requires maxRetries=0") {
    testErrorAddAndRemove(
      getTestTransactionStateRow(compatibleTableProperties, maxRetries = 1),
      testDataFileStatusWithStatistics,
      partitionValues = Collections.emptyMap(),
      dataChange = true,
      "GenerateIcebergCompatActionUtils requires maxRetries=0")
  }

  test("GenerateIcebergCompatActionUtils requires icebergWriterCompatV1") {
    // Not set at all
    testErrorAddAndRemove(
      getTestTransactionStateRow(tblProperties = Map()),
      testDataFileStatusWithStatistics,
      partitionValues = Collections.emptyMap(),
      dataChange = true,
      "only supported on tables with 'delta.enableIcebergWriterCompatV1' set to true")
    // Set to false
    testErrorAddAndRemove(
      getTestTransactionStateRow(tblProperties = Map(
        TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.getKey -> "FALSE",
        TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true",
        TableConfig.COLUMN_MAPPING_MODE.getKey -> "id")),
      testDataFileStatusWithStatistics,
      partitionValues = Collections.emptyMap(),
      dataChange = true,
      "only supported on tables with 'delta.enableIcebergWriterCompatV1' set to true")
  }

  test("GenerateIcebergCompatActionUtils doesn't support partitioned tables") {
    testErrorAddAndRemove(
      getTestTransactionStateRow(compatibleTableProperties, partitionColumns = Seq("id")),
      testDataFileStatusWithStatistics,
      partitionValues = Map("id" -> Literal.ofInt(1)).asJava,
      dataChange = true,
      "GenerateIcebergCompatActionUtils is not supported for partitioned tables")
  }

  test("GenerateIcebergCompatActionUtils requires statistics in add files") {
    assert(
      intercept[KernelException] {
        GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1AddAction(
          getTestTransactionStateRow(compatibleTableProperties),
          testDataFileStatusWithoutStatistics,
          Collections.emptyMap(), // partitionValues
          true // dataChange
        )
      }.getMessage.contains("icebergCompatV2 compatibility requires 'numRecords' statistic"))
  }

  test("GenerateIcebergCompatActionUtils cannot create remove with dataChange=true " +
    "for append-only table") {
    assert(
      intercept[KernelException] {
        GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1RemoveAction(
          getTestTransactionStateRow(
            compatibleTableProperties ++ Map(TableConfig.APPEND_ONLY_ENABLED.getKey -> "true")),
          testDataFileStatusWithStatistics,
          Collections.emptyMap(), // partitionValues
          true // dataChange
        )
      }.getMessage.contains("Cannot modify append only table"))
  }

  /* ----- Valid cases ----- */

  private def validateAddAction(
      row: Row,
      expectedPath: String,
      // expectedPartitionValues - for now this is not supported as anything other than empty
      expectedSize: Long,
      expectedModificationTime: Long,
      expectedDataChange: Boolean,
      expectedStatsString: String): Unit = {
    assert(row.getSchema == SingleAction.FULL_SCHEMA)
    (0 until SingleAction.FULL_SCHEMA.length()).foreach { idx =>
      if (idx == SingleAction.ADD_FILE_ORDINAL) {
        assert(!row.isNullAt(idx))
      } else {
        assert(row.isNullAt(idx))
      }
    }
    val addRow = row.getStruct(SingleAction.ADD_FILE_ORDINAL)
    assert(addRow.getSchema == AddFile.FULL_SCHEMA)

    val addFile = new AddFile(addRow)
    assert(addFile.getPath == expectedPath)
    assert(addFile.getPartitionValues.getSize == 0)
    assert(addFile.getSize == expectedSize)
    assert(addFile.getModificationTime == expectedModificationTime)
    assert(addFile.getDataChange == expectedDataChange)
    assert(!addFile.getTags.isPresent)
    assert(!addFile.getBaseRowId.isPresent)
    assert(!addFile.getDefaultRowCommitVersion.isPresent)
    assert(!addFile.getDeletionVector.isPresent)
    // We have to do our stats check differently since the AddFile::getStats API does not fully
    // deserialize the statistics (only grabs the numRecords field)
    assert(addRow.getString(AddFile.FULL_SCHEMA.indexOf("stats")) == expectedStatsString)
  }

  private def validateRemoveAction(
      row: Row,
      expectedPath: String,
      // expectedPartitionValues - for now this is not supported as anything other than empty
      expectedSize: Long,
      expectedDeletionTimestamp: Long,
      expectedDataChange: Boolean,
      expectedStatsString: Option[String]): Unit = {
    assert(row.getSchema == SingleAction.FULL_SCHEMA)
    (0 until SingleAction.FULL_SCHEMA.length()).foreach { idx =>
      if (idx == SingleAction.REMOVE_FILE_ORDINAL) {
        assert(!row.isNullAt(idx))
      } else {
        assert(row.isNullAt(idx))
      }
    }
    val removeRow = row.getStruct(SingleAction.REMOVE_FILE_ORDINAL)
    assert(removeRow.getSchema == RemoveFile.FULL_SCHEMA)

    val removeFile = new RemoveFile(removeRow)
    assert(removeFile.getPath == expectedPath)
    assert(removeFile.getDeletionTimestamp.isPresent &&
      removeFile.getDeletionTimestamp.get == expectedDeletionTimestamp)
    assert(removeFile.getDataChange == expectedDataChange)
    assert(removeFile.getExtendedFileMetadata.isPresent && removeFile.getExtendedFileMetadata.get)
    assert(
      removeFile.getPartitionValues.isPresent && removeFile.getPartitionValues.get.getSize == 0)
    assert(removeFile.getSize.isPresent && removeFile.getSize.get == expectedSize)
    if (expectedStatsString.nonEmpty) {
      // We have to do our stats check differently since the RemoveFile::getStats API does not fully
      // deserialize the statistics (only grabs the numRecords field)
      assert(
        removeRow.getString(RemoveFile.FULL_SCHEMA.indexOf("stats")) == expectedStatsString.get)
    } else {
      assert(removeRow.isNullAt(RemoveFile.FULL_SCHEMA.indexOf("stats")))
    }
    assert(!removeFile.getTags.isPresent)
    assert(!removeFile.getDeletionVector.isPresent)
    assert(!removeFile.getBaseRowId.isPresent)
    assert(!removeFile.getDefaultRowCommitVersion.isPresent)
  }

  test("generateIcebergCompatWriterV1AddAction creates correct add row") {
    Seq(true, false).foreach { dataChange =>
      val txnRow = getTestTransactionStateRow(compatibleTableProperties)
      val statsString = testDataFileStatusWithStatistics.getStatistics.get
        .serializeAsJson(TransactionStateRow.getPhysicalSchema(txnRow))
      validateAddAction(
        GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1AddAction(
          txnRow,
          testDataFileStatusWithStatistics,
          Collections.emptyMap(), // partitionValues
          dataChange),
        expectedPath = "file1.parquet",
        expectedSize = 1000,
        expectedModificationTime = 10,
        expectedDataChange = dataChange,
        expectedStatsString = statsString)
    }
  }

  test("generateIcebergCompatWriterV1AddAction creates correct remove row") {
    Seq(true, false).foreach { dataChange =>
      // RemoveFiles are allowed to be missing statistics (where as AddFiles are not)
      Seq(testDataFileStatusWithStatistics, testDataFileStatusWithoutStatistics).foreach {
        fileStatus =>
          val txnRow = getTestTransactionStateRow(compatibleTableProperties)
          val statsString = if (fileStatus.getStatistics.isPresent) {
            Some(fileStatus.getStatistics.get
              .serializeAsJson(TransactionStateRow.getPhysicalSchema(txnRow)))
          } else {
            None
          }
          validateRemoveAction(
            GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1RemoveAction(
              txnRow,
              fileStatus,
              Collections.emptyMap(), // partitionValues
              dataChange),
            expectedPath = "file1.parquet",
            expectedSize = 1000,
            expectedDeletionTimestamp = 10,
            expectedDataChange = dataChange,
            expectedStatsString = statsString)
      }
    }
  }
}

object GenerateIcebergCompatActionUtilsSuite {

  private val testDataFileStatusWithStatistics = new DataFileStatus(
    "/test/table/path/file1.parquet",
    1000,
    10,
    Optional.of(
      new DataFileStatistics(
        100,
        Map(new Column("id") -> Literal.ofInt(0)).asJava,
        Map(new Column("id") -> Literal.ofInt(10)).asJava,
        Map(new Column("id") -> java.lang.Long.valueOf(0)).asJava)))

  private val testDataFileStatusWithoutStatistics = new DataFileStatus(
    "/test/table/path/file1.parquet",
    1000,
    10,
    Optional.empty())

  private val testSchema = new StructType()
    .add("id", IntegerType.INTEGER)
    .add("comment", StringType.STRING)

  private val testTablePath = "/test/table/path"

  private val compatibleTableProperties = Map(
    TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.getKey -> "true",
    TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true",
    TableConfig.COLUMN_MAPPING_MODE.getKey -> "id")
}
