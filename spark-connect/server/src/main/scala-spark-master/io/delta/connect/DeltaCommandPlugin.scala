/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package org.apache.spark.sql.connect.delta

import scala.collection.JavaConverters._

import com.google.protobuf
import io.delta.connect.proto
import io.delta.connect.spark.{proto => spark_proto}
import io.delta.tables.DeltaTable

import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, InvalidPlanInput}
import org.apache.spark.sql.connect.delta.ImplicitProtoConversions._
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.plugin.CommandPlugin

/**
 * Planner plugin for command extensions using [[proto.DeltaCommand]].
 */
class DeltaCommandPlugin extends CommandPlugin with DeltaPlannerBase {
  override def process(raw: Array[Byte], planner: SparkConnectPlanner): Boolean = {
    val command = protobuf.Any.parseFrom(raw)
    if (command.is(classOf[proto.DeltaCommand])) {
      process(command.unpack(classOf[proto.DeltaCommand]), planner)
      true
    } else {
      false
    }
  }

  private def process(command: proto.DeltaCommand, planner: SparkConnectPlanner): Unit = {
    command.getCommandTypeCase match {
      case proto.DeltaCommand.CommandTypeCase.CLONE_TABLE =>
        processCloneTable(planner, command.getCloneTable)
      case proto.DeltaCommand.CommandTypeCase.VACUUM_TABLE =>
        processVacuumTable(planner, command.getVacuumTable)
      case proto.DeltaCommand.CommandTypeCase.UPGRADE_TABLE_PROTOCOL =>
        processUpgradeTableProtocol(planner, command.getUpgradeTableProtocol)
      case proto.DeltaCommand.CommandTypeCase.GENERATE =>
        processGenerate(planner, command.getGenerate)
      case proto.DeltaCommand.CommandTypeCase.CREATE_DELTA_TABLE =>
        processCreateDeltaTable(planner, command.getCreateDeltaTable)
      case proto.DeltaCommand.CommandTypeCase.ADD_FEATURE_SUPPORT =>
        processAddFeatureSupport(planner, command.getAddFeatureSupport)
      case proto.DeltaCommand.CommandTypeCase.DROP_FEATURE_SUPPORT =>
        processDropFeatureSupport(planner, command.getDropFeatureSupport)
      case _ =>
        throw InvalidPlanInput(s"${command.getCommandTypeCase}")
    }
  }


  private def processCloneTable(
      planner: SparkConnectPlanner, cloneTable: proto.CloneTable): Unit = {
    val deltaTable = transformDeltaTable(planner, cloneTable.getTable)
    if (cloneTable.hasVersion) {
      deltaTable.cloneAtVersion(
        cloneTable.getVersion,
        cloneTable.getTarget,
        cloneTable.getIsShallow,
        cloneTable.getReplace,
        cloneTable.getPropertiesMap.asScala.toMap
      )
    } else if (cloneTable.hasTimestamp) {
      deltaTable.cloneAtTimestamp(
        cloneTable.getTimestamp,
        cloneTable.getTarget,
        cloneTable.getIsShallow,
        cloneTable.getReplace,
        cloneTable.getPropertiesMap.asScala.toMap
      )
    } else {
      deltaTable.clone(
        cloneTable.getTarget,
        cloneTable.getIsShallow,
        cloneTable.getReplace,
        cloneTable.getPropertiesMap.asScala.toMap
      )
    }
  }

  private def processVacuumTable(planner: SparkConnectPlanner, vacuum: proto.VacuumTable): Unit = {
    val deltaTable = transformDeltaTable(planner, vacuum.getTable)
    if (vacuum.hasRetentionHours) {
      deltaTable.vacuum(vacuum.getRetentionHours)
    } else {
      deltaTable.vacuum()
    }
  }

  private def processUpgradeTableProtocol(
      planner: SparkConnectPlanner, upgradeTableProtocol: proto.UpgradeTableProtocol): Unit = {
    val deltaTable = transformDeltaTable(planner, upgradeTableProtocol.getTable)
    deltaTable.upgradeTableProtocol(
      upgradeTableProtocol.getReaderVersion, upgradeTableProtocol.getWriterVersion)
  }

  private def processGenerate(planner: SparkConnectPlanner, generate: proto.Generate): Unit = {
    val deltaTable = transformDeltaTable(planner, generate.getTable)
    deltaTable.generate(generate.getMode)
  }

  private def processCreateDeltaTable(
      planner: SparkConnectPlanner, createDeltaTable: proto.CreateDeltaTable): Unit = {
    val spark = planner.session
    val tableBuilder = createDeltaTable.getMode match {
      case proto.CreateDeltaTable.Mode.MODE_CREATE =>
        DeltaTable.create(spark)
      case proto.CreateDeltaTable.Mode.MODE_CREATE_IF_NOT_EXISTS =>
        DeltaTable.createIfNotExists(spark)
      case proto.CreateDeltaTable.Mode.MODE_REPLACE =>
        DeltaTable.replace(spark)
      case proto.CreateDeltaTable.Mode.MODE_CREATE_OR_REPLACE =>
        DeltaTable.createOrReplace(spark)
      case _ =>
        throw new Exception("Unsupported table creation mode")
    }
    if (createDeltaTable.hasTableName) {
      tableBuilder.tableName(createDeltaTable.getTableName)
    }
    if (createDeltaTable.hasLocation) {
      tableBuilder.location(createDeltaTable.getLocation)
    }
    if (createDeltaTable.hasComment) {
      tableBuilder.comment(createDeltaTable.getComment)
    }
    for (column <- createDeltaTable.getColumnsList.asScala) {
      val colBuilder = DeltaTable
        .columnBuilder(spark, column.getName)
        .nullable(column.getNullable)
      if (column.getDataType.getKindCase == spark_proto.DataType.KindCase.UNPARSED) {
        colBuilder.dataType(column.getDataType.getUnparsed.getDataTypeString)
      } else {
        colBuilder.dataType(DataTypeProtoConverter.toCatalystType(column.getDataType))
      }
      if (column.hasGeneratedAlwaysAs) {
        colBuilder.generatedAlwaysAs(column.getGeneratedAlwaysAs)
      }
      if (column.hasIdentityInfo) {
        val identityInfo = column.getIdentityInfo
        if (identityInfo.getAllowExplicitInsert) {
          colBuilder.generatedByDefaultAsIdentity(identityInfo.getStart, identityInfo.getStep)
        } else {
          colBuilder.generatedAlwaysAsIdentity(identityInfo.getStart, identityInfo.getStep)
        }
      }
      if (column.hasComment) {
        colBuilder.comment(column.getComment)
      }
      tableBuilder.addColumn(colBuilder.build())
    }
    val partitioningColumns = createDeltaTable.getPartitioningColumnsList.asScala.toSeq
    if (!partitioningColumns.isEmpty) {
      tableBuilder.partitionedBy(partitioningColumns: _*)
    }
    val clusteringColumns = createDeltaTable.getClusteringColumnsList.asScala.toSeq
    if (!clusteringColumns.isEmpty) {
      tableBuilder.clusterBy(clusteringColumns: _*)
    }
    for ((key, value) <- createDeltaTable.getPropertiesMap.asScala) {
      tableBuilder.property(key, value)
    }
    tableBuilder.execute()
  }

  private def processAddFeatureSupport(
      planner: SparkConnectPlanner, addFeatureSupport: proto.AddFeatureSupport): Unit = {
    val deltaTable = transformDeltaTable(planner, addFeatureSupport.getTable)
    deltaTable.addFeatureSupport(addFeatureSupport.getFeatureName)
  }

  private def processDropFeatureSupport(
      planner: SparkConnectPlanner, dropFeatureSupport: proto.DropFeatureSupport): Unit = {
    val deltaTable = transformDeltaTable(planner, dropFeatureSupport.getTable)
    if (dropFeatureSupport.hasTruncateHistory) {
      deltaTable.dropFeatureSupport(
        dropFeatureSupport.getFeatureName,
        dropFeatureSupport.getTruncateHistory)
    } else {
      deltaTable.dropFeatureSupport(dropFeatureSupport.getFeatureName)
    }
  }
}
