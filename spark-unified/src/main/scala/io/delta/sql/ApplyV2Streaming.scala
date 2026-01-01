/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.sql

import scala.jdk.CollectionConverters._

import io.delta.kernel.spark.catalog.SparkTable
import io.delta.kernel.spark.utils.{CatalogTableUtils, ScalaUtils}
import org.apache.spark.sql.delta.sources.{DeltaSQLConfV2, DeltaSourceUtils}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

/**
 * Rule for applying the V2 streaming path by rewriting V1 StreamingRelation
 * with Delta DataSource to StreamingRelationV2 with SparkTable.
 *
 * This rule handles the case where Spark's FindDataSourceTable rule has converted
 * a StreamingRelationV2 (with DeltaTableV2) back to a StreamingRelation because
 * DeltaTableV2 doesn't advertise STREAMING_READ capability. We convert it back to
 * StreamingRelationV2 with SparkTable (from kernel-spark) which does support streaming.
 *
 * Behavior based on spark.databricks.delta.v2.enableMode:
 * - AUTO (default): Only applies to Unity Catalog managed tables
 * - NONE: Rule is disabled, no conversion happens
 *
 * @param session The Spark session for configuration access
 */
class ApplyV2Streaming(
    @transient private val session: SparkSession)
  extends Rule[LogicalPlan] {

  private def isDeltaStreamingRelation(s: StreamingRelation): Boolean = {
    // Check if this is a Delta streaming relation by examining:
    // 1. The source name (e.g., "delta" from .format("delta"))
    // 2. The catalog table's provider (e.g., "DELTA" from Unity Catalog)
    // 3. Whether the table is a Unity Catalog managed table
    s.dataSource.catalogTable match {
      case Some(catalogTable) =>
        DeltaSourceUtils.isDeltaDataSourceName(s.sourceName) ||
        catalogTable.provider.exists(DeltaSourceUtils.isDeltaDataSourceName) ||
        CatalogTableUtils.isUnityCatalogManagedTable(catalogTable)
      case None => false
    }
  }

  private def shouldApplyV2Streaming(s: StreamingRelation): Boolean = {
    if (!isDeltaStreamingRelation(s)) {
      return false
    }

    val mode = session.conf.get(
      DeltaSQLConfV2.V2_ENABLE_MODE.key,
      DeltaSQLConfV2.V2_ENABLE_MODE.defaultValueString)

    mode.toUpperCase(java.util.Locale.ROOT) match {
      case "AUTO" =>
        // Only apply for Unity Catalog managed tables
        // catalogTable is guaranteed to be Some because isDeltaStreamingRelation checked it
        s.dataSource.catalogTable.exists(CatalogTableUtils.isUnityCatalogManagedTable)
      case "NONE" | _ =>
        // V2 streaming disabled or other mode
        false
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case s: StreamingRelation if shouldApplyV2Streaming(s) =>
      // catalogTable is guaranteed to be defined because shouldApplyV2Streaming checks it
      // via isDeltaStreamingRelation.
      val catalogTable = s.dataSource.catalogTable.get
      val ident =
        Identifier.of(catalogTable.identifier.database.toArray, catalogTable.identifier.table)
      val table =
        new SparkTable(
          ident,
          catalogTable,
          ScalaUtils.toJavaMap(catalogTable.properties))

      StreamingRelationV2(
        source = None,
        sourceName = "delta",
        table = table,
        extraOptions = new CaseInsensitiveStringMap(s.dataSource.options.asJava),
        output = toAttributes(table.schema),
        catalog = None,
        identifier = Some(ident),
        v1Relation = Some(s))
  }
}

