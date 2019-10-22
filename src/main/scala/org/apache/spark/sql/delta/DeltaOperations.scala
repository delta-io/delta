/*
 * Copyright 2019 Databricks, Inc.
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

import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Exhaustive list of operations that can be performed on a Delta table. These operations are
 * tracked as the first line in delta logs, and power `DESCRIBE HISTORY` for Delta tables.
 */
object DeltaOperations {

  /**
   * An operation that can be performed on a Delta table.
   * @param name The name of the operation.
   */
  sealed abstract class Operation(val name: String) {
    val parameters: Map[String, Any]

    lazy val jsonEncodedValues: Map[String, String] = parameters.mapValues(JsonUtils.toJson(_))
  }

  /** Recorded during batch inserts. Predicates can be provided for overwrites. */
  case class Write(
      mode: SaveMode,
      partitionBy: Option[Seq[String]] = None,
      predicate: Option[String] = None) extends Operation("WRITE") {
    override val parameters: Map[String, Any] = Map("mode" -> mode.name()) ++
      partitionBy.map("partitionBy" -> JsonUtils.toJson(_)) ++
      predicate.map("predicate" -> _)
  }
  /** Recorded during streaming inserts. */
  case class StreamingUpdate(
      outputMode: OutputMode,
      queryId: String,
      epochId: Long) extends Operation("STREAMING UPDATE") {
    override val parameters: Map[String, Any] =
      Map("outputMode" -> outputMode.toString, "queryId" -> queryId, "epochId" -> epochId.toString)
  }
  /** Recorded while deleting certain partitions. */
  case class Delete(predicate: Seq[String]) extends Operation("DELETE") {
    override val parameters: Map[String, Any] = Map("predicate" -> JsonUtils.toJson(predicate))
  }
  /** Recorded when truncating the table. */
  case class Truncate() extends Operation("TRUNCATE") {
    override val parameters: Map[String, Any] = Map.empty
  }
  /** Recorded when fscking the table. */
  case class Fsck(numRemovedFiles: Long) extends Operation("FSCK") {
    override val parameters: Map[String, Any] = Map(
      "numRemovedFiles" -> numRemovedFiles
    )
  }
  /** Recorded when converting a table into a Delta table. */
  case class Convert(
      numFiles: Long,
      partitionBy: Seq[String],
      collectStats: Boolean,
      catalogTable: Option[String]) extends Operation("CONVERT") {
    override val parameters: Map[String, Any] = Map(
      "numFiles" -> numFiles,
      "partitionedBy" -> JsonUtils.toJson(partitionBy),
      "collectStats" -> collectStats) ++ catalogTable.map("catalogTable" -> _)
  }
  /** Recorded when optimizing the table. */
  case class Optimize(
      predicate: Seq[String],
      zOrderBy: Seq[String],
      batchId: Int,
      auto: Boolean) extends Operation("OPTIMIZE") {
    override val parameters: Map[String, Any] = Map(
      "predicate" -> JsonUtils.toJson(predicate),
      "zOrderBy" -> JsonUtils.toJson(zOrderBy),
      "batchId" -> JsonUtils.toJson(batchId),
      "auto" -> auto
    )
  }
  /** Recorded when a merge operation is committed to the table. */
  case class Merge(
      predicate: Option[String],
      updatePredicate: Option[String],
      deletePredicate: Option[String],
      insertPredicate: Option[String]) extends Operation("MERGE") {
    override val parameters: Map[String, Any] = {
      predicate.map("predicate" -> _).toMap ++
        updatePredicate.map("updatePredicate" -> _).toMap ++
        deletePredicate.map("deletePredicate" -> _).toMap ++
        insertPredicate.map("insertPredicate" -> _).toMap
    }
  }
  /** Recorded when an update operation is committed to the table. */
  case class Update(predicate: Option[String]) extends Operation("UPDATE") {
    override val parameters: Map[String, Any] = predicate.map("predicate" -> _).toMap
  }
  /** Recorded when the table is created. */
  case class CreateTable(metadata: Metadata, isManaged: Boolean, asSelect: Boolean = false)
      extends Operation("CREATE TABLE" + s"${if (asSelect) " AS SELECT" else ""}") {
    override val parameters: Map[String, Any] = Map(
      "isManaged" -> isManaged.toString,
      "description" -> Option(metadata.description),
      "partitionBy" -> JsonUtils.toJson(metadata.partitionColumns),
      "properties" -> JsonUtils.toJson(metadata.configuration))
  }
  /** Recorded when the table properties are set. */
  case class SetTableProperties(
      properties: Map[String, String]) extends Operation("SET TBLPROPERTIES") {
    override val parameters: Map[String, Any] = Map("properties" -> JsonUtils.toJson(properties))
  }
  /** Recorded when the table properties are unset. */
  case class UnsetTableProperties(
      propKeys: Seq[String],
      ifExists: Boolean) extends Operation("UNSET TBLPROPERTIES") {
    override val parameters: Map[String, Any] = Map(
      "properties" -> JsonUtils.toJson(propKeys),
      "ifExists" -> ifExists)
  }
  /** Recorded when columns are added. */
  case class AddColumns(
      colsToAdd: Seq[QualifiedColTypeWithPositionForLog]) extends Operation("ADD COLUMNS") {

    override val parameters: Map[String, Any] = Map(
      "columns" -> JsonUtils.toJson(colsToAdd.map {
        case QualifiedColTypeWithPositionForLog(columnPath, column, colPosition) =>
          Map(
            "column" -> structFieldToMap(columnPath, column)
          ) ++ colPosition.map("position" -> _.toString)
      }))
  }
  /** Recorded when columns are changed. */
  case class ChangeColumn(
      columnPath: Seq[String],
      columnName: String,
      newColumn: StructField,
      colPosition: Option[String]) extends Operation("CHANGE COLUMN") {

    override val parameters: Map[String, Any] = Map(
      "column" -> JsonUtils.toJson(structFieldToMap(columnPath, newColumn))
    ) ++ colPosition.map("position" -> _)
  }
  /** Recorded when columns are replaced. */
  case class ReplaceColumns(
      columns: Seq[StructField]) extends Operation("REPLACE COLUMNS") {

    override val parameters: Map[String, Any] = Map(
      "columns" -> JsonUtils.toJson(columns.map(structFieldToMap(Seq.empty, _))))
  }

  case class UpgradeProtocol(newProtocol: Protocol) extends Operation("UPGRADE PROTOCOL") {
    override val parameters: Map[String, Any] = Map("newProtocol" -> JsonUtils.toJson(Map(
      "minReaderVersion" -> newProtocol.minReaderVersion,
      "minWriterVersion" -> newProtocol.minWriterVersion
    )))
  }

  object ManualUpdate extends Operation("Manual Update") {
    override val parameters: Map[String, Any] = Map.empty
  }

  object FileNotificationRetention extends Operation("FILE NOTIFICATION RETENTION") {
    override val parameters: Map[String, Any] = Map.empty
  }

  case class UpdateColumnMetadata(
      operationName: String,
      columns: Seq[(Seq[String], StructField)])
    extends Operation(operationName) {
    override val parameters: Map[String, Any] = {
      Map("columns" -> JsonUtils.toJson(columns.map {
        case (path, field) => structFieldToMap(path, field)
      }))
    }
  }

  /** Recorded when recomputing stats on the table. */
  case class ComputeStats(predicate: Seq[String]) extends Operation("COMPUTE STATS") {
    override val parameters: Map[String, Any] = Map(
      "predicate" -> JsonUtils.toJson(predicate))
  }

  /** Recorded when manually re-/un-/setting ZCube Information for existing files. */
  case class ResetZCubeInfo(predicate: Seq[String], zOrderBy: Seq[String])
    extends Operation("RESET ZCUBE INFO") {

    override val parameters: Map[String, Any] = Map(
      "predicate" -> JsonUtils.toJson(predicate),
      "zOrderBy" -> JsonUtils.toJson(zOrderBy))
  }

  case class UpdateSchema(oldSchema: StructType, newSchema: StructType)
      extends Operation("UPDATE SCHEMA") {
    override val parameters: Map[String, Any] = Map(
      "oldSchema" -> JsonUtils.toJson(oldSchema),
      "newSchema" -> JsonUtils.toJson(newSchema))
  }

  private def structFieldToMap(colPath: Seq[String], field: StructField): Map[String, Any] = {
    Map(
      "name" -> UnresolvedAttribute(colPath :+ field.name).name,
      "type" -> field.dataType.typeName,
      "nullable" -> field.nullable,
      "metadata" -> JsonUtils.mapper.readValue[Map[String, Any]](field.metadata.json)
    )
  }

  /**
   * Qualified column type with position. We define a copy of the type here to avoid depending on
   * the parser output classes in our logging.
   */
  case class QualifiedColTypeWithPositionForLog(
     columnPath: Seq[String],
     column: StructField,
     colPosition: Option[String])
}
