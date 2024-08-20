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

import io.delta.connect.spark.{proto => delta_spark_proto}

import org.apache.spark.connect.{proto => spark_proto}
import org.apache.spark.sql.connect.ConnectProtoUtils

object ImplicitProtoConversions {
  implicit def convertRelationToSpark(
      relation: delta_spark_proto.Relation): spark_proto.Relation = {
    ConnectProtoUtils.parseRelationWithRecursionLimit(relation.toByteArray, recursionLimit = 1024)
  }

  implicit def convertRelationToDelta(
      relation: spark_proto.Relation): delta_spark_proto.Relation = {
    // TODO: Recursion limits
    delta_spark_proto.Relation.parseFrom(relation.toByteArray)
  }

  implicit def convertCommandToSpark(command: delta_spark_proto.Command): spark_proto.Command = {
    ConnectProtoUtils.parseCommandWithRecursionLimit(command.toByteArray, recursionLimit = 1024)
  }

  implicit def convertCommandToDelta(command: spark_proto.Command): delta_spark_proto.Command = {
    // TODO: Recursion limits
    delta_spark_proto.Command.parseFrom(command.toByteArray)
  }

  implicit def convertExpressionToSpark(
      expr: delta_spark_proto.Expression): spark_proto.Expression = {
    ConnectProtoUtils.parseExpressionWithRecursionLimit(expr.toByteArray, recursionLimit = 1024)
  }

  implicit def convertExpressionToDelta(
      expr: spark_proto.Expression): delta_spark_proto.Expression = {
    // TODO: Recursion limits
    delta_spark_proto.Expression.parseFrom(expr.toByteArray)
  }

  implicit def convertDataTypeToSpark(
      dataType: delta_spark_proto.DataType): spark_proto.DataType = {
    ConnectProtoUtils.parseDataTypeWithRecursionLimit(dataType.toByteArray, recursionLimit = 1024)
  }

  implicit def convertDataTypeToDelta(
      dataType: spark_proto.DataType): delta_spark_proto.DataType = {
    // TODO: Recursion limits
    delta_spark_proto.DataType.parseFrom(dataType.toByteArray)
  }
}
