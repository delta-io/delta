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

import java.util.Optional

import scala.collection.JavaConverters._

import com.google.protobuf
import com.google.protobuf.{ByteString, InvalidProtocolBufferException}
import io.delta.connect.proto
import io.delta.tables.DeltaTable

import org.apache.spark.SparkEnv
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.classic.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, InvalidPlanInput}
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.delta.DeltaRelationPlugin.{parseAnyFrom, parseRelationFrom}
import org.apache.spark.sql.connect.delta.ImplicitProtoConversions._
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.plugin.RelationPlugin
import org.apache.spark.sql.delta.commands.ConvertToDeltaCommand
import org.apache.spark.sql.types.StructType

/**
 * Planner plugin for relation extensions using [[proto.DeltaRelation]].
 */
class DeltaRelationPlugin extends RelationPlugin with DeltaPlannerBase {
  override def transform(raw: Array[Byte], planner: SparkConnectPlanner): Optional[LogicalPlan] = {
    val relation = parseAnyFrom(raw,
      SparkEnv.get.conf.get(Connect.CONNECT_GRPC_MARSHALLER_RECURSION_LIMIT))
    if (relation.is(classOf[proto.DeltaRelation])) {
      Optional.of(
        transform(
          parseRelationFrom(relation.getValue,
            SparkEnv.get.conf.get(Connect.CONNECT_GRPC_MARSHALLER_RECURSION_LIMIT)),
          planner
        ))
    } else {
      Optional.empty()
    }
  }


  private def transform(
      relation: proto.DeltaRelation, planner: SparkConnectPlanner): LogicalPlan = {
    relation.getRelationTypeCase match {
      case proto.DeltaRelation.RelationTypeCase.SCAN =>
        transformScan(planner.session, relation.getScan)
      case proto.DeltaRelation.RelationTypeCase.DESCRIBE_HISTORY =>
        transformDescribeHistory(planner.session, relation.getDescribeHistory)
      case proto.DeltaRelation.RelationTypeCase.DESCRIBE_DETAIL =>
        transformDescribeDetail(planner.session, relation.getDescribeDetail)
      case proto.DeltaRelation.RelationTypeCase.CONVERT_TO_DELTA =>
        transformConvertToDelta(planner.session, relation.getConvertToDelta)
      case proto.DeltaRelation.RelationTypeCase.RESTORE_TABLE =>
        transformRestoreTable(planner.session, relation.getRestoreTable)
      case proto.DeltaRelation.RelationTypeCase.IS_DELTA_TABLE =>
        transformIsDeltaTable(planner.session, relation.getIsDeltaTable)
      case proto.DeltaRelation.RelationTypeCase.DELETE_FROM_TABLE =>
        transformDeleteFromTable(planner, relation.getDeleteFromTable)
      case proto.DeltaRelation.RelationTypeCase.UPDATE_TABLE =>
        transformUpdateTable(planner, relation.getUpdateTable)
      case _ =>
        throw InvalidPlanInput(s"Unknown DeltaRelation ${relation.getRelationTypeCase}")
    }
  }

  private def transformScan(spark: SparkSession, scan: proto.Scan): LogicalPlan = {
    val deltaTable = transformDeltaTable(spark, scan.getTable)
    deltaTable.toDF.queryExecution.analyzed
  }

  private def transformDescribeHistory(
      spark: SparkSession, describeHistory: proto.DescribeHistory): LogicalPlan = {
    val deltaTable = transformDeltaTable(spark, describeHistory.getTable)
    deltaTable.history().queryExecution.analyzed
  }

  private def transformDescribeDetail(
      spark: SparkSession, describeDetail: proto.DescribeDetail): LogicalPlan = {
    val deltaTable = transformDeltaTable(spark, describeDetail.getTable)
    deltaTable.detail().queryExecution.analyzed
  }

  private def transformConvertToDelta(
      spark: SparkSession, convertToDelta: proto.ConvertToDelta): LogicalPlan = {
    val tableIdentifier =
      spark.sessionState.sqlParser.parseTableIdentifier(convertToDelta.getIdentifier)
    val partitionSchema = if (convertToDelta.hasPartitionSchemaStruct) {
      Some(DataTypeProtoConverter.toCatalystType(convertToDelta.getPartitionSchemaStruct)
        .asInstanceOf[StructType])
    } else if (convertToDelta.hasPartitionSchemaString) {
      Some(StructType.fromDDL(convertToDelta.getPartitionSchemaString))
    } else {
      None
    }

    val cvt = ConvertToDeltaCommand(
      tableIdentifier,
      partitionSchema,
      collectStats = true,
      deltaPath = None)
    cvt.run(spark)

    val result = if (cvt.isCatalogTable(spark.sessionState.analyzer, tableIdentifier)) {
      convertToDelta.getIdentifier
    } else {
      s"delta.`${tableIdentifier.table}`"
    }
    spark.createDataset(result :: Nil)(Encoders.STRING).queryExecution.analyzed
  }

  private def transformRestoreTable(
      spark: SparkSession, restoreTable: proto.RestoreTable): LogicalPlan = {
    val deltaTable = transformDeltaTable(spark, restoreTable.getTable)
    val df = if (restoreTable.hasVersion) {
      deltaTable.restoreToVersion(restoreTable.getVersion)
    } else if (restoreTable.hasTimestamp) {
      deltaTable.restoreToTimestamp(restoreTable.getTimestamp)
    } else {
      throw new RuntimeException()
    }
    df.queryExecution.commandExecuted
  }

  private def transformIsDeltaTable(
      spark: SparkSession, isDeltaTable: proto.IsDeltaTable): LogicalPlan = {
    val result = DeltaTable.isDeltaTable(spark, isDeltaTable.getPath)
    spark.createDataset(result :: Nil)(Encoders.scalaBoolean).queryExecution.analyzed
  }

  private def transformDeleteFromTable(
      planner: SparkConnectPlanner, deleteFromTable: proto.DeleteFromTable): LogicalPlan = {
    val target = planner.transformRelation(deleteFromTable.getTarget)
    val condition = if (deleteFromTable.hasCondition) {
      Some(planner.transformExpression(deleteFromTable.getCondition))
    } else {
      None
    }
    Dataset.ofRows(
        planner.session, DeleteFromTable(target, condition.getOrElse(Literal.TrueLiteral)))
      .queryExecution.commandExecuted
  }

  private def transformUpdateTable(
      planner: SparkConnectPlanner, updateTable: proto.UpdateTable): LogicalPlan = {
    val target = planner.transformRelation(updateTable.getTarget)
    val condition = if (updateTable.hasCondition) {
      Some(planner.transformExpression(updateTable.getCondition))
    } else {
      None
    }
    val assignments = updateTable.getAssignmentsList.asScala.map(transformAssignment(planner, _))
    Dataset.ofRows(planner.session, UpdateTable(target, assignments.toSeq, condition))
      .queryExecution.commandExecuted
  }

  private def transformAssignment(
      planner: SparkConnectPlanner, assignment: proto.Assignment): Assignment = {
    Assignment(
      key = planner.transformExpression(assignment.getField),
      value = planner.transformExpression(assignment.getValue))
  }
}

object DeltaRelationPlugin {
  private def parseAnyFrom(ba: Array[Byte], recursionLimit: Int): protobuf.Any = {
    val bs = ByteString.copyFrom(ba)
    val cis = bs.newCodedInput()
    cis.setSizeLimit(Integer.MAX_VALUE)
    cis.setRecursionLimit(recursionLimit)
    val plan = protobuf.Any.parseFrom(cis)
    try {
      // If the last tag is 0, it means the message is correctly parsed.
      // If the last tag is not 0, it means the message is not correctly
      // parsed, and we should throw an exception.
      cis.checkLastTagWas(0)
      plan
    } catch {
      case e: InvalidProtocolBufferException =>
        e.setUnfinishedMessage(plan)
        throw e
    }
  }

  private def parseRelationFrom(bs: ByteString, recursionLimit: Int): proto.DeltaRelation = {
    val cis = bs.newCodedInput()
    cis.setSizeLimit(Integer.MAX_VALUE)
    cis.setRecursionLimit(recursionLimit)
    val plan = proto.DeltaRelation.parseFrom(cis)
    try {
      // If the last tag is 0, it means the message is correctly parsed.
      // If the last tag is not 0, it means the message is not correctly
      // parsed, and we should throw an exception.
      cis.checkLastTagWas(0)
      plan
    } catch {
      case e: InvalidProtocolBufferException =>
        e.setUnfinishedMessage(plan)
        throw e
    }
  }
}
