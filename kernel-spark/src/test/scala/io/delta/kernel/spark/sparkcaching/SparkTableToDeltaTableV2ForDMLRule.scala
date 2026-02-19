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

package io.delta.kernel.spark.sparkcaching

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, DeleteFromTable, LogicalPlan, OverwriteByExpression, UpdateTable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.MergeIntoCommand
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import io.delta.kernel.spark.table.SparkTable

/**
 * Test helper rule that converts SparkTable → DeltaTableV2 for DML operations.
 *
 * This enables testing of the cache invalidation bug where:
 * 1. A DataFrame is read using V2 (DataSourceV2Relation(SparkTable)) and cached
 * 2. A DML operation (UPDATE, DELETE, MERGE, INSERT, OVERWRITE) is performed
 * 3. This rule converts SparkTable → DeltaTableV2 for DML operations
 * 4. DeltaAnalysis then converts DeltaTableV2 → LogicalRelation (V1 fallback)
 * 5. Cache invalidation fails: DataSourceV2Relation.sameResult(LogicalRelation) returns false
 */
class SparkTableToDeltaTableV2ForDMLRule(spark: SparkSession)
  extends Rule[LogicalPlan]
  with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!isDMLOperation(plan)) {
      return plan
    }

    // Use transformUpWithNewOutput to handle attribute ID remapping automatically
    plan.transformUpWithNewOutput {
      case append: AppendData if append.table.isInstanceOf[DataSourceV2Relation] =>
        val rel = append.table.asInstanceOf[DataSourceV2Relation]
        if (rel.table.isInstanceOf[SparkTable] || rel.table.isInstanceOf[CacheAwareSparkTable]) {
          val newRel = convertSparkTableToDeltaTableV2(rel)
          (append.copy(table = newRel), Seq.empty)
        } else {
          (append, Seq.empty)
        }

      case update: UpdateTable =>
        // UpdateTable.table is wrapped in SubqueryAlias, which will be handled by the catch-all
        // case below. We don't convert here to avoid attribute mismatch issues.
        (update, Seq.empty)

      case delete: DeleteFromTable =>
        // DeleteFromTable.table will be handled by the catch-all
        (delete, Seq.empty)

      case overwrite: OverwriteByExpression =>
        // OverwriteByExpression.table will be handled by the catch-all
        (overwrite, Seq.empty)

      // Catch-all: Convert any DataSourceV2Relation(SparkTable) to DeltaTableV2
      // This is SAFE because isDMLOperation() ensures we only run during DML operations,
      // not for pure SELECT queries. This handles relations that appear anywhere in the
      // DML tree, including wrapped in SubqueryAlias.
      case rel: DataSourceV2Relation 
          if rel.table.isInstanceOf[SparkTable] || 
             rel.table.isInstanceOf[CacheAwareSparkTable] =>
        val newRel = convertSparkTableToDeltaTableV2(rel)
        val attrMapping = rel.output.zip(newRel.output)
        (newRel, attrMapping)
    }
  }

  private def isDMLOperation(plan: LogicalPlan): Boolean = {
    plan.exists {
      case _: UpdateTable | _: DeleteFromTable | _: MergeIntoCommand |
           _: AppendData | _: OverwriteByExpression => true
      case _ => false
    }
  }

  private def convertSparkTableToDeltaTableV2(
      rel: DataSourceV2Relation): DataSourceV2Relation = {
    // Extract the underlying SparkTable and table path
    val (sparkTable, tablePath) = rel.table match {
      case cacheAware: CacheAwareSparkTable =>
        // CacheAwareSparkTable stores tablePath as a private field
        val path = try {
          val field = cacheAware.getClass.getDeclaredField("tablePath")
          field.setAccessible(true)
          field.get(cacheAware).asInstanceOf[String]
        } catch {
          case e: Exception =>
            logError(s"Failed to get tablePath from CacheAwareSparkTable: ${e.getMessage}")
            throw new IllegalStateException(
              s"Could not extract table path from CacheAwareSparkTable: ${e.getMessage}", e)
        }
        // Also extract the underlying SparkTable for completeness
        val underlying = try {
          val field = cacheAware.getClass.getDeclaredField("underlying")
          field.setAccessible(true)
          field.get(cacheAware).asInstanceOf[SparkTable]
        } catch {
          case _: Exception => null // Not critical, we have the path
        }
        (underlying, path)
      
      case spark: SparkTable =>
        // SparkTable stores tablePath in a private field, use reflection to access it
        val path = try {
          val field = spark.getClass.getDeclaredField("tablePath")
          field.setAccessible(true)
          field.get(spark).asInstanceOf[String]
        } catch {
          case e: Exception =>
            logError(s"Failed to get tablePath from SparkTable: ${e.getMessage}")
            throw new IllegalStateException(
              s"Could not extract table path from SparkTable: ${e.getMessage}", e)
        }
        (spark, path)
      
      case other =>
        throw new IllegalStateException(
          s"Expected SparkTable or CacheAwareSparkTable, got: ${other.getClass.getName}")
    }

    val deltaTableV2 = DeltaTableV2(
      spark = spark,
      path = new Path(tablePath),
      catalogTable = None,
      tableIdentifier = rel.identifier.map(_.toString),
      options = rel.options.asScala.toMap
    )

    DataSourceV2Relation.create(deltaTableV2, rel.catalog, rel.identifier, rel.options)
  }
}

/**
 * SparkSession extension to inject the SparkTable → DeltaTableV2 conversion rule.
 */
class SparkTableToDeltaTableV2Extension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectResolutionRule { session =>
      new SparkTableToDeltaTableV2ForDMLRule(session)
    }
  }
}
