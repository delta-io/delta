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

package org.apache.spark.sql.delta

import scala.reflect.runtime.universe._

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.{QueryTest, SaveMode}
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * Tests for the correct serialization and deserialization of CommitInfo.
 * The main focus is on the correct deserialization of operation parameters.
 * See [[JsonMapDeserializer]] for more details about how operation
 * parameter deserialization was broken before.
 */
class CommitInfoSerializerSuite extends QueryTest with SharedSparkSession {

  def testOperationSerialization(operation: DeltaOperations.Operation): Unit = {
    val commitInfo = CommitInfo(
      time = 123L,
      operation = operation.name,
      inCommitTimestamp = Some(123L),
      operationParameters = Map.empty,
      commandContext = Map("clusterId" -> "23"),
      readVersion = Some(23),
      isolationLevel = Some("SnapshotIsolation"),
      isBlindAppend = Some(true),
      operationMetrics = Some(Map("m1" -> "v1", "m2" -> "v2")),
      userMetadata = Some("123"),
      tags = Some(Map("k1" -> "v1")),
      txnId = Some("123")
    ).copy(engineInfo = None)

    val inMemoryCommitInfo = commitInfo.copy(operationParameters = operation.jsonEncodedValues)
    val commitInfoSerialized = inMemoryCommitInfo.json
    val roundTrippedCommitInfo = Action.fromJson(commitInfoSerialized).asInstanceOf[CommitInfo]
    assert(roundTrippedCommitInfo.operationParameters == inMemoryCommitInfo.operationParameters)
    assert(roundTrippedCommitInfo == inMemoryCommitInfo)

    // Also ensure that CommitInfo.getLegacyOperationParameters is correct
    val legacyPostDeserializationCommitInfo =
      JsonUtils.mapper.readValue[ActionWrapper](commitInfoSerialized).commitInfo
    val legacyOperationParametersActual =
      roundTrippedCommitInfo.getLegacyPostDeserializationOperationParameters
    assert(
      legacyOperationParametersActual == legacyPostDeserializationCommitInfo.operationParameters)
  }

  val testMetadata = Metadata(
    id = "test-id",
    name = "test_table",
    description = "Test table",
    format = Format(),
    schemaString = StructType(Seq(
      StructField("col1", StringType),
      StructField("col2", IntegerType)
    )).json,
    partitionColumns = Seq("col1"),
    configuration = Map("property1" -> "value1")
  )

  val oldSchema = StructType(Seq(StructField("col1", StringType)))
  val newSchema = StructType(Seq(StructField("col1", StringType), StructField("col2", IntegerType)))

  val trackedOperationClasses = Map(
    "Convert" -> (() => DeltaOperations.Convert(
      numFiles = 23L,
      partitionBy = Seq("a", "b"),
      collectStats = false,
      catalogTable = Some("t1"),
      sourceFormat = Some("parquet"))),
    "DomainMetadataCleanup" -> (() => DeltaOperations.DomainMetadataCleanup(1)),
    "Write" -> (() => DeltaOperations.Write(
      mode = SaveMode.Append,
      partitionBy = Some(Seq("col1", "col2")),
      predicate = Some("col1 > 10"),
      userMetadata = Some("test metadata")
    )),
    "StreamingUpdate" -> (() => DeltaOperations.StreamingUpdate(
      outputMode = OutputMode.Append(),
      queryId = "query-123",
      epochId = 42L,
      userMetadata = Some("streaming metadata")
    )),
    "Delete" -> (() => DeltaOperations.Delete(Seq(EqualTo(Literal("col1"), Literal("value1"))))),
    "Truncate" -> (() => DeltaOperations.Truncate()),
    "Merge" -> (() => DeltaOperations.Merge(
      predicate = Some(EqualTo(Literal("source.id"), Literal("target.id"))),
      updatePredicate = Some("source.value > target.value"),
      deletePredicate = Some("source.flag = 'delete'"),
      insertPredicate = Some("source.id IS NOT NULL"),
      matchedPredicates = Seq(DeltaOperations.MergePredicate(Some("matched"), "update")),
      notMatchedPredicates = Seq(DeltaOperations.MergePredicate(Some("not matched"), "insert")),
      notMatchedBySourcePredicates = Seq(
        DeltaOperations.MergePredicate(Some("not matched by source"), "delete"))
    )),
    "Update" -> (() => DeltaOperations.Update(Some(EqualTo(Literal("col1"), Literal("value1"))))),
    "CreateTable" -> (() => DeltaOperations.CreateTable(
      metadata = testMetadata,
      isManaged = true,
      asSelect = true,
      clusterBy = Some(Seq("col1"))
    )),
    "ReplaceTable" -> (() => DeltaOperations.ReplaceTable(
      metadata = testMetadata,
      isManaged = true,
      orCreate = true,
      asSelect = true,
      userMetadata = Some("replace metadata"),
      clusterBy = Some(Seq("col2")))),
    "SetTableProperties" ->
      (() => DeltaOperations.SetTableProperties(Map("key1" -> "value1", "key2" -> "value2"))),
    "UnsetTableProperties" ->
      (() => DeltaOperations.UnsetTableProperties(Seq("key1", "key2"), ifExists = true)),
    "DropTableFeature" ->
      (() => DeltaOperations.DropTableFeature("testFeature", truncateHistory = true)),
    "AddColumns" -> (() => DeltaOperations.AddColumns(Seq(
      DeltaOperations.QualifiedColTypeWithPositionForLog(
        Seq("newCol"),
        StructField("newCol", StringType),
        Some("AFTER col1"))))),
    "DropColumns" -> (() => DeltaOperations.DropColumns(Seq(Seq("col1"), Seq("col2")))),
    "RenameColumn" -> (() => DeltaOperations.RenameColumn(Seq("oldCol"), Seq("newCol"))),
    "ChangeColumn" -> (() => DeltaOperations.ChangeColumn(
      columnPath = Seq("col1"),
      columnName = "col1",
      newColumn = StructField("col1", StringType),
      colPosition = Some("FIRST"))),
    "ChangeColumns" -> (() => DeltaOperations.ChangeColumns(Seq(
      DeltaOperations.ChangeColumn(
        columnPath = Seq("col1"),
        columnName = "col1",
        newColumn = StructField("col1", StringType),
        colPosition = Some("FIRST"))))),
    "ReplaceColumns" -> (() => DeltaOperations.ReplaceColumns(Seq(
      StructField("newCol1", StringType),
      StructField("newCol2", IntegerType)))),
    "UpgradeProtocol" ->
      (() => DeltaOperations.UpgradeProtocol(Protocol(minReaderVersion = 1, minWriterVersion = 2))),
    "UpdateColumnMetadata" -> (() => DeltaOperations.UpdateColumnMetadata(
      "UPDATE COLUMN METADATA",
      Seq((Seq("col1"), StructField("col1", StringType))))),
    "UpdateSchema" -> (() => DeltaOperations.UpdateSchema(oldSchema, newSchema)),
    "AddConstraint" -> (() => DeltaOperations.AddConstraint("check_positive", "col1 > 0")),
    "DropConstraint" -> (() => DeltaOperations.DropConstraint("check_positive", Some("col1 > 0"))),
    "ComputeStats" ->
      (() => DeltaOperations.ComputeStats(Seq(EqualTo(Literal("col1"), Literal("value1"))))),
    "Restore" -> (() => DeltaOperations.Restore(Some(5L), Some("2023-01-01T00:00:00Z"))),
    "Optimize" -> (() => DeltaOperations.Optimize(
      predicate = Seq(EqualTo(Literal("col1"), Literal("value1"))),
      zOrderBy = Seq("col1", "col2"),
      auto = true,
      clusterBy = Some(Seq("col3")),
      isFull = false)),
    "Clone" -> (() => DeltaOperations.Clone(
      source = "s3://bucket/path/to/table",
      sourceVersion = 10L)),
    "VacuumStart" -> (() => DeltaOperations.VacuumStart(
      retentionCheckEnabled = true,
      specifiedRetentionMillis = Some(604800000L),
      defaultRetentionMillis = 604800000L)),
    "VacuumEnd" -> (() => DeltaOperations.VacuumEnd("COMPLETED")),
    "Reorg" -> (() => DeltaOperations.Reorg(
      predicate = Seq(EqualTo(Literal("col1"), Literal("value1"))),
      applyPurge = true)),
    "ClusterBy" -> (() => DeltaOperations.ClusterBy(
      oldClusteringColumns = JsonUtils.toJson(Seq("oldCol1", "oldCol2")),
      newClusteringColumns = JsonUtils.toJson(Seq("newCol1", "newCol2")))),
    "RowTrackingBackfill" -> (() => DeltaOperations.RowTrackingBackfill(batchId = 3)),
    "RowTrackingUnBackfill" -> (() => DeltaOperations.RowTrackingUnBackfill(batchId = 4)),
    "UpgradeUniformProperties" ->
      (() => DeltaOperations.UpgradeUniformProperties(Map("uniform.property1" -> "value1"))),
    "RemoveColumnMapping" ->
      (() => DeltaOperations.RemoveColumnMapping(Some("remove column mapping metadata"))),
    "AddDeletionVectorsTombstones" -> (() => DeltaOperations.AddDeletionVectorsTombstones),
    "ManualUpdate" -> (() => DeltaOperations.ManualUpdate),
    "EmptyCommit" -> (() => DeltaOperations.EmptyCommit)
  )

  trackedOperationClasses.foreach { case (operationName, operationGenerator) =>
    test(s"$operationName operation serialization") {
      testOperationSerialization(operationGenerator())
    }
  }

  val ignoredOperationClasses = Set(
    "TestOperation"
  )

  test("all operations should be tested in this suite") {
    val mirror = runtimeMirror(getClass.getClassLoader)
    val moduleSymbol =
      mirror.staticModule("org.apache.spark.sql.delta.DeltaOperations")
    val moduleMirror = mirror.reflectModule(moduleSymbol)
    val instance = moduleMirror.instance

    val instanceMirror = mirror.reflect(instance)
    val symbol = instanceMirror.symbol
    val traitOperation =
      typeOf[org.apache.spark.sql.delta.DeltaOperations.Operation].typeSymbol

    val allOperations = symbol.typeSignature.members.flatMap {
      case cls: ClassSymbol
          if cls.isCaseClass && cls.isPublic && cls.toType.baseClasses.contains(traitOperation) =>
        Some(cls.name.toString)
      case obj: ModuleSymbol
          if obj.isPublic && obj.moduleClass.asType.toType.baseClasses.contains(traitOperation) =>
        Some(obj.name.toString)
      case _ => None
    }.toSet
    assert(
      (allOperations -- ignoredOperationClasses) == trackedOperationClasses.keySet,
      s"if you add a new operation, please add a new test case in this suite " +
        "for that operation and then add the operation name to the `trackedOperationClasses` " +
        "Map in this test. Missing operations: " +
        s"${allOperations -- ignoredOperationClasses -- trackedOperationClasses.keySet}"
    )
  }
}

/**
 * A minimal CommitInfo with only operation parameters. This one
 * does not use the custom JsonMapDeserializer so we can
 * use it to test our ability to generate the legacy operation parameters.
 */
case class LegacyCommitInfoWithOperationParametersOnly(
  operationParameters: Map[String, String]
)

case class ActionWrapper(commitInfo: LegacyCommitInfoWithOperationParametersOnly = null)
