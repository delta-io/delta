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

import io.delta.kernel.spark.table.SparkTable
import io.delta.kernel.spark.utils.{CatalogTableUtils, ScalaUtils}

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * An extension for Spark SQL to activate Delta SQL parser to support Delta SQL grammar.
 *
 * Scala example to create a `SparkSession` with the Delta SQL parser:
 * {{{
 *    import org.apache.spark.sql.SparkSession
 *
 *    val spark = SparkSession
 *       .builder()
 *       .appName("...")
 *       .master("...")
 *       .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
 *       .getOrCreate()
 * }}}
 *
 * Java example to create a `SparkSession` with the Delta SQL parser:
 * {{{
 *    import org.apache.spark.sql.SparkSession;
 *
 *    SparkSession spark = SparkSession
 *                 .builder()
 *                 .appName("...")
 *                 .master("...")
 *                 .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
 *                 .getOrCreate();
 * }}}
 *
 * Python example to create a `SparkSession` with the Delta SQL parser (PySpark doesn't pick up the
 * SQL conf "spark.sql.extensions" in Apache Spark 2.4.x, hence we need to activate it manually in
 * 2.4.x. However, because `SparkSession` has been created and everything has been materialized, we
 * need to clone a new session to trigger the initialization. See SPARK-25003):
 * {{{
 *    from pyspark.sql import SparkSession
 *
 *    spark = SparkSession \
 *        .builder \
 *        .appName("...") \
 *        .master("...") \
 *        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
 *        .getOrCreate()
 *    if spark.sparkContext().version < "3.":
 *        spark.sparkContext()._jvm.io.delta.sql.DeltaSparkSessionExtension() \
 *            .apply(spark._jsparkSession.extensions())
 *        spark = SparkSession(spark.sparkContext(), spark._jsparkSession.cloneSession())
 * }}}
 *
 * @since 0.4.0
 */
class DeltaSparkSessionExtension extends AbstractDeltaSparkSessionExtension {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // First register all the base Delta rules from the V1 implementation.
    super.apply(extensions)

    // Register a post-hoc resolution rule that rewrites V1 StreamingRelation plans that
    // read Delta tables into V2 StreamingRelationV2 plans backed by SparkTable.
    //
    // NOTE: The current implementation is a no-op placeholder to keep binary compatibility
    // and provide an explicit hook for the upcoming V2 streaming application work.
    extensions.injectResolutionRule { session =>
      new ApplyV2Streaming(session)
    }
  }

  /**
   * Rule for applying the V2 streaming path by rewriting V1 StreamingRelation
   * with Delta DataSource to StreamingRelationV2 with SparkTable.
   *
   * This rule handles the case where Spark's FindDataSourceTable rule has converted
   * a StreamingRelationV2 (with DeltaTableV2) back to a StreamingRelation because
   * DeltaTableV2 doesn't advertise STREAMING_READ capability. We convert it back to
   * StreamingRelationV2 with SparkTable (from kernel-spark) which does support streaming.
   */
  class ApplyV2Streaming(
      @transient private val session: org.apache.spark.sql.SparkSession)
    extends Rule[LogicalPlan] {

    private def isDeltaStreamingRelation(s: StreamingRelation): Boolean = {
      // Check if this is a Delta streaming relation by examining:
      // 1. The source name (e.g., "delta" from .format("delta"))
      // 2. The catalog table's provider (e.g., "DELTA" from Unity Catalog)
      // 3. Whether the table is a Unity Catalog managed table
      s.dataSource.catalogTable.isDefined && (
        DeltaSourceUtils.isDeltaDataSourceName(s.sourceName) ||
        s.dataSource.catalogTable.get.provider.exists(DeltaSourceUtils.isDeltaDataSourceName) ||
        CatalogTableUtils.isUnityCatalogManagedTable(s.dataSource.catalogTable.get)
      )
    }

    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
      case s: StreamingRelation if isDeltaStreamingRelation(s) =>
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

  /**
   * NoOpRule for binary compatibility with Delta 3.3.0
   * This class must remain here to satisfy MiMa checks
   */
  class NoOpRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan
  }
}
