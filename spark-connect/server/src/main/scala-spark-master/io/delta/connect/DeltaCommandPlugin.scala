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

import org.apache.spark.sql.connect.common.InvalidPlanInput
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
}
