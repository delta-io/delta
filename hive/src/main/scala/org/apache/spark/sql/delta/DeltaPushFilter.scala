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

import scala.collection.immutable.HashSet
import scala.collection.JavaConverters._

import org.apache.hadoop.hive.ql.exec.{FunctionRegistry, SerializationUtilities}
import org.apache.hadoop.hive.ql.lib._
import org.apache.hadoop.hive.ql.parse.SemanticException
import org.apache.hadoop.hive.ql.plan.{ExprNodeColumnDesc, ExprNodeConstantDesc, ExprNodeGenericFuncDesc}
import org.apache.hadoop.hive.ql.udf.generic._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{And, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, InSet, LessThan, LessThanOrEqual, Like, Literal, Not}

object DeltaPushFilter extends Logging {
  lazy val supportedPushDownUDFs = Array(
    "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual",
    "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan",
    "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan",
    "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan",
    "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan",
    "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual",
    "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualNS",
    "org.apache.hadoop.hive.ql.udf.UDFLike",
    "org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn"
  )

  def partitionFilterConverter(hiveFilterExprSeriablized: String): Seq[Expression] = {
    if (hiveFilterExprSeriablized != null) {
      val filterExpr = SerializationUtilities.deserializeExpression(hiveFilterExprSeriablized)
      val opRules = new java.util.LinkedHashMap[Rule, NodeProcessor]()
      val nodeProcessor = new NodeProcessor() {
        @throws[SemanticException]
        def process(nd: Node, stack: java.util.Stack[Node],
            procCtx: NodeProcessorCtx, nodeOutputs: Object*): Object = {
          nd match {
            case e: ExprNodeGenericFuncDesc if FunctionRegistry.isOpAnd(e) =>
              nodeOutputs.map(_.asInstanceOf[Expression]).reduce(And)
            case e: ExprNodeGenericFuncDesc =>
              val (columnDesc, constantDesc) =
                if (nd.getChildren.get(0).isInstanceOf[ExprNodeColumnDesc]) {
                  (nd.getChildren.get(0), nd.getChildren.get(1))
                } else { (nd.getChildren.get(1), nd.getChildren.get(0)) }

              val columnAttr = UnresolvedAttribute(
                columnDesc.asInstanceOf[ExprNodeColumnDesc].getColumn)
              val constantVal = Literal(constantDesc.asInstanceOf[ExprNodeConstantDesc].getValue)
              nd.asInstanceOf[ExprNodeGenericFuncDesc].getGenericUDF match {
                case f: GenericUDFOPNotEqualNS =>
                  Not(EqualNullSafe(columnAttr, constantVal))
                case f: GenericUDFOPNotEqual =>
                  Not(EqualTo(columnAttr, constantVal))
                case f: GenericUDFOPEqualNS =>
                  EqualNullSafe(columnAttr, constantVal)
                case f: GenericUDFOPEqual =>
                  EqualTo(columnAttr, constantVal)
                case f: GenericUDFOPGreaterThan =>
                  GreaterThan(columnAttr, constantVal)
                case f: GenericUDFOPEqualOrGreaterThan =>
                  GreaterThanOrEqual(columnAttr, constantVal)
                case f: GenericUDFOPLessThan =>
                  LessThan(columnAttr, constantVal)
                case f: GenericUDFOPEqualOrLessThan =>
                  LessThanOrEqual(columnAttr, constantVal)
                case f: GenericUDFBridge if f.getUdfName.equals("like") =>
                  Like(columnAttr, constantVal)
                case f: GenericUDFIn =>
                  val inConstantVals = nd.getChildren.asScala
                    .filter(_.isInstanceOf[ExprNodeConstantDesc])
                    .map(_.asInstanceOf[ExprNodeConstantDesc].getValue)
                    .map(Literal(_)).toSet
                  InSet(columnAttr, HashSet() ++ inConstantVals)
                case _ =>
                  throw new RuntimeException(s"Unsupported func(${nd.getName}) " +
                    s"which can not be pushed down to delta")
              }
            case _ => null
          }
        }
      }

      val disp = new DefaultRuleDispatcher(nodeProcessor, opRules, null)
      val ogw = new DefaultGraphWalker(disp)
      val topNodes = new java.util.ArrayList[Node]()
      topNodes.add(filterExpr)
      val nodeOutput = new java.util.HashMap[Node, Object]()
      try {
        ogw.startWalking(topNodes, nodeOutput)
      } catch {
        case ex: Exception =>
          throw new RuntimeException(ex)
      }
      logInfo(s"converted partition filter expr:" +
        s"${nodeOutput.get(filterExpr).asInstanceOf[Expression].toJSON}")
      Seq(nodeOutput.get(filterExpr).asInstanceOf[Expression])
    } else Seq.empty[org.apache.spark.sql.catalyst.expressions.Expression]
  }
}
