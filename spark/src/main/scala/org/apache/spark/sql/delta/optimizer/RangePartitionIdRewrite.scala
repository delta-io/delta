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

package org.apache.spark.sql.delta.optimizer

import org.apache.spark.sql.delta.expressions.{PartitionerExpr, RangePartitionId}

import org.apache.spark.{RangePartitioner, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, IsNotNull, SortOrder}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.util.MutablePair


/**
 * Rewrites all [[RangePartitionId]] into [[PartitionerExpr]] by running sampling jobs
 * on the child RDD in order to determine the range boundaries.
 */
case class RangePartitionIdRewrite(session: SparkSession)
  extends Rule[LogicalPlan] {
  import RangePartitionIdRewrite._

  private def sampleSizeHint: Int = conf.rangeExchangeSampleSizePerPartition

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case node: UnaryNode => node.transformExpressionsUp {
      case RangePartitionId(expr, n) =>
        val aliasedExpr = Alias(expr, "__RPI_child_col__")()
        val exprAttr = aliasedExpr.toAttribute

        val planForSampling = Filter(IsNotNull(exprAttr), Project(Seq(aliasedExpr), node.child))
        val qeForSampling = new QueryExecution(session, planForSampling)

        val desc = s"RangePartitionId($expr, $n) sampling"
        val jobGroupId = session.sparkContext.getLocalProperty(SparkContext.SPARK_JOB_GROUP_ID)
        withCallSite(session.sparkContext, desc) {
          SQLExecution.withNewExecutionId(qeForSampling) {
            withJobGroup(session.sparkContext, jobGroupId, desc) {
              // The code below is inspired from ShuffleExchangeExec.prepareShuffleDependency()

              // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
              // partition bounds. To get accurate samples, we need to copy the mutable keys.
              val rddForSampling = qeForSampling.toRdd.mapPartitionsInternal { iter =>
                val mutablePair = new MutablePair[InternalRow, Null]()
                iter.map(row => mutablePair.update(row.copy(), null))
              }

              val sortOrder = SortOrder(exprAttr, Ascending)
              implicit val ordering = new LazilyGeneratedOrdering(Seq(sortOrder), Seq(exprAttr))
              val partitioner = new RangePartitioner(n, rddForSampling, true, sampleSizeHint)

              PartitionerExpr(expr, partitioner)
            }
          }
        }
    }
  }
}

object RangePartitionIdRewrite {
  /**
   * Executes the equivalent [[SparkContext.setJobGroup()]] call, runs the given `body`,
   * then restores the original jobGroup.
   */
  private def withJobGroup[T](
      sparkContext: SparkContext,
      groupId: String,
      description: String)
      (body: => T): T = {
    val oldJobDesc = sparkContext.getLocalProperty("spark.job.description")
    val oldGroupId = sparkContext.getLocalProperty("spark.jobGroup.id")
    val oldJobInterrupt = sparkContext.getLocalProperty("spark.job.interruptOnCancel")
    sparkContext.setJobGroup(groupId, description, interruptOnCancel = true)
    try body finally {
      sparkContext.setJobGroup(
        oldGroupId, oldJobDesc, Option(oldJobInterrupt).map(_.toBoolean).getOrElse(false))
    }
  }

  /**
   * Executes the equivalent setCallSite() call, runs the given `body`,
   * then restores the original call site.
   */
  private def withCallSite[T](sparkContext: SparkContext, shortCallSite: String)(body: => T): T = {
    val oldCallSiteShortForm = sparkContext.getLocalProperty("callSite.short")
    val oldCallSiteLongForm = sparkContext.getLocalProperty("callSite.long")
    sparkContext.setCallSite(shortCallSite)
    try body finally {
      sparkContext.setLocalProperty("callSite.short", oldCallSiteShortForm)
      sparkContext.setLocalProperty("callSite.long", oldCallSiteLongForm)
    }
  }
}
