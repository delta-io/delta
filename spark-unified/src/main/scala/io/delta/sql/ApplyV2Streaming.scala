/*
 * Copyright (2026) The Delta Lake Project Authors.
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
import scala.jdk.OptionConverters._

import io.delta.spark.internal.v2.catalog.SparkTable
import io.delta.spark.internal.v2.utils.ScalaUtils
import org.apache.spark.sql.delta.DeltaV2Mode
import org.apache.spark.sql.delta.sources.DeltaSourceUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.Relocated.StreamingRelation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Rule for applying the V2 streaming path by rewriting V1 StreamingRelation
 * with Delta DataSource to StreamingRelationV2 with SparkTable.
 *
 * This rule handles the case where Spark's FindDataSourceTable rule has converted
 * a StreamingRelationV2 (with DeltaTableV2) back to a StreamingRelation because
 * DeltaTableV2 doesn't advertise STREAMING_READ capability. We convert it back to
 * StreamingRelationV2 with SparkTable (from sparkV2) which does support streaming.
 *
 * See [[DeltaV2Mode]] for configuration behavior.
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
    s.dataSource.catalogTable match {
      case Some(catalogTable) =>
        DeltaSourceUtils.isDeltaDataSourceName(s.sourceName) ||
        catalogTable.provider.exists(DeltaSourceUtils.isDeltaDataSourceName)
      case None => false
    }
  }

  private def shouldApplyV2Streaming(s: StreamingRelation): Boolean = {
    if (!isDeltaStreamingRelation(s)) {
      return false
    }

    val deltaV2Mode = new DeltaV2Mode(session.sessionState.conf)
    deltaV2Mode.isStreamingReadsEnabled(s.dataSource.catalogTable.toJava)
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
          // Use user-specified streaming options to override catalog storage properties.
          // SparkTable handles merging catalogTable storage props internally.
          ScalaUtils.toJavaMap(s.dataSource.options))
      val catalog = catalogTable.identifier.catalog.map(
        session.sessionState.catalogManager.catalog)


      StreamingRelationV2(
        // We rebuild this from the resolved V2 table, so there is no V1 source to carry through.
        // This is only non-None when StreamingRelationV2 is created by wrapping a V1 streaming
        // data source; in that case Spark keeps the underlying V1 DataSource instance here.
        source = None,
        sourceName = DeltaSourceUtils.NAME,
        table = table,
        extraOptions = new CaseInsensitiveStringMap(s.dataSource.options.asJava),
        output = toAttributes(table.schema),
        catalog = catalog,
        identifier = Some(ident),
        // Keep this None to force the V2 path; we don't want to fall back to V1 here.
        v1Relation = None)
  }
}
