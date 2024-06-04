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

import com.google.protobuf
import com.google.protobuf.{ByteString, InvalidProtocolBufferException}
import io.delta.connect.proto

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connect.common.InvalidPlanInput
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.delta.DeltaRelationPlugin.{parseAnyFrom, parseRelationFrom}
import org.apache.spark.sql.connect.delta.ImplicitProtoConversions._
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.plugin.RelationPlugin

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
      case _ =>
        throw InvalidPlanInput(s"Unknown DeltaRelation ${relation.getRelationTypeCase}")
    }
  }

  private def transformScan(spark: SparkSession, scan: proto.Scan): LogicalPlan = {
    val deltaTable = transformDeltaTable(spark, scan.getTable)
    deltaTable.toDF.queryExecution.analyzed
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
