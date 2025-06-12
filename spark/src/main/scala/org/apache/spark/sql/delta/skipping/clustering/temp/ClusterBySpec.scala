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

package org.apache.spark.sql.delta.skipping.clustering.temp

import scala.reflect.ClassTag

import org.apache.spark.sql.delta.skipping.clustering.ClusteredTableUtils
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}
import org.antlr.v4.runtime.ParserRuleContext

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface, ParserUtils}
import org.apache.spark.sql.catalyst.plans.logical.{CreateTable, CreateTableAsSelect, LeafNode, LogicalPlan, ReplaceTable, ReplaceTableAsSelect}
import org.apache.spark.sql.connector.expressions.{BucketTransform, FieldReference, NamedReference, Transform}

/**
 * A container for clustering information. Copied from OSS Spark.
 *
 * This class will be removed when we integrate with OSS Spark's CLUSTER BY implementation.
 * @see https://github.com/apache/spark/pull/42577
 *
 * @param columnNames the names of the columns used for clustering.
 */
case class ClusterBySpec(columnNames: Seq[NamedReference]) {
  override def toString: String = toJson

  def toJson: String =
    ClusterBySpec.mapper.writeValueAsString(columnNames.map(_.fieldNames))
}

object ClusterBySpec {
  private val mapper = {
    val ret = new ObjectMapper() with ClassTagExtensions
    ret.setSerializationInclusion(Include.NON_ABSENT)
    ret.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    ret.registerModule(DefaultScalaModule)
    ret
  }

  // ClassTag is added to avoid the "same type after erasure" issue with the case class.
  def apply[_: ClassTag](columnNames: Seq[Seq[String]]): ClusterBySpec = {
    ClusterBySpec(columnNames.map(FieldReference(_)))
  }

  // Convert from table property back to ClusterBySpec.
  def fromProperty(columns: String): ClusterBySpec = {
    ClusterBySpec(mapper.readValue[Seq[Seq[String]]](columns).map(FieldReference(_)))
  }

  def toProperty(clusterBySpec: ClusterBySpec): (String, String) = {
    ClusteredTableUtils.PROP_CLUSTERING_COLUMNS -> clusterBySpec.toJson
  }

  def fromProperties(properties: Map[String, String]): Option[ClusterBySpec] = {
    properties.get(ClusteredTableUtils.PROP_CLUSTERING_COLUMNS).map { clusteringColumns =>
      fromProperty(clusteringColumns)
    }
  }

  def toProperties(clusterBySpec: ClusterBySpec): Map[String, String] = {
    val columnValue = mapper.writeValueAsString(clusterBySpec.columnNames.map(_.fieldNames))
    Map(ClusteredTableUtils.PROP_CLUSTERING_COLUMNS -> columnValue)
  }

  def fromColumnNames(names: Seq[String]): ClusterBySpec = {
    ClusterBySpec(names.map(FieldReference(_)))
  }
}

/**
 * A [[LogicalPlan]] representing a CLUSTER BY clause.
 *
 * This class will be removed when we integrate with OSS Spark's CLUSTER BY implementation.
 * @see https://github.com/apache/spark/pull/42577
 *
 * @param clusterBySpec: clusterBySpec which contains the clustering columns.
 * @param startIndex: start index of CLUSTER BY clause.
 * @param stopIndex: stop index of CLUSTER BY clause.
 * @param parenStartIndex: start index of the left parenthesis in CLUSTER BY clause.
 * @param parenStopIndex: stop index of the right parenthesis in CLUSTER BY clause.
 * @param ctx: parser rule context of the CLUSTER BY clause.
 */
case class ClusterByPlan(
    clusterBySpec: ClusterBySpec,
    startIndex: Int,
    stopIndex: Int,
    parenStartIndex: Int,
    parenStopIndex: Int,
    ctx: ParserRuleContext)
    extends LeafNode {
  override def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this
  override def output: Seq[Attribute] = Seq.empty
}

/**
 * Parser utils for parsing a [[ClusterByPlan]] and converts it to table properties.
 *
 * This class will be removed when we integrate with OSS Spark's CLUSTER BY implementation.
 * @see https://github.com/apache/spark/pull/42577
 *
 * @param clusterByPlan: the ClusterByPlan to parse.
 * @param delegate: delegate parser.
 */
case class ClusterByParserUtils(clusterByPlan: ClusterByPlan, delegate: ParserInterface) {
  // Update partitioning to include clustering columns as transforms.
  private def updatePartitioning(partitioning: Seq[Transform]): Seq[Transform] = {
    // Validate no bucketing is specified.
    if (partitioning.exists(t => t.isInstanceOf[BucketTransform])) {
      ParserUtils.operationNotAllowed(
        "Clustering and bucketing cannot both be specified. " +
          "Please remove CLUSTERED BY INTO BUCKETS if you " +
          "want to create a Delta table with clustering",
        clusterByPlan.ctx)
    }
    Seq(ClusterByTransform(clusterByPlan.clusterBySpec.columnNames))
  }

  /**
   * Parse the [[ClusterByPlan]] by replacing CLUSTER BY with PARTITIONED BY and
   * leverage Spark SQL parser to perform the validation. After parsing, store the
   * clustering columns in the logical plan's partitioning transforms.
   *
   * @param sqlText: original SQL text.
   * @return the logical plan after parsing.
   */
  def parsePlan(sqlText: String): LogicalPlan = {
    val colText =
      sqlText.substring(clusterByPlan.parenStartIndex, clusterByPlan.parenStopIndex + 1)
    // Replace CLUSTER BY with PARTITIONED BY to let SparkSqlParser do the validation for us.
    // This serves as a short-term workaround until Spark incorporates CREATE TABLE ... CLUSTER BY
    // syntax.
    val partitionedByText = "PARTITIONED BY " + colText
    val newSqlText =
      sqlText.substring(0, clusterByPlan.startIndex) +
        partitionedByText +
        sqlText.substring(clusterByPlan.stopIndex + 1)
    try {
      delegate.parsePlan(newSqlText) match {
        case create: CreateTable =>
          create.copy(partitioning = updatePartitioning(create.partitioning))
        case ctas: CreateTableAsSelect =>
          ctas.copy(partitioning = updatePartitioning(ctas.partitioning))
        case replace: ReplaceTable =>
          replace.copy(partitioning = updatePartitioning(replace.partitioning))
        case rtas: ReplaceTableAsSelect =>
          rtas.copy(partitioning = updatePartitioning(rtas.partitioning))
        case plan => plan
      }
    } catch {
      case e: ParseException if (e.errorClass.contains("DUPLICATE_CLAUSES")) =>
        // Since we replace CLUSTER BY with PARTITIONED BY, duplicated clauses means we
        // encountered CLUSTER BY with PARTITIONED BY.
        ParserUtils.operationNotAllowed(
          "Clustering and partitioning cannot both be specified. " +
            "Please remove PARTITIONED BY if you want to create a Delta table with clustering",
          clusterByPlan.ctx)
    }
  }
}
