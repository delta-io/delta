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

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.ConvertSparkTableToDeltaTableV2
import org.apache.spark.sql.delta.sources.DeltaSQLConfV2

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

  /**
   * Injects the SparkTable to DeltaTableV2 conversion rule if V2 connector is enabled
   * (STRICT mode). This rule must run after PreprocessTimeTravel but before DeltaAnalysis
   * to ensure SparkTable instances are converted to DeltaTableV2 after TimeTravel nodes
   * are resolved but before Delta-specific analysis rules process them.
   *
   * The conversion is only active when spark.databricks.delta.v2.enableMode is set to
   * "STRICT".
   */
  override protected def injectPreDeltaAnalysisRules(
      extensions: SparkSessionExtensions): Boolean = {
    extensions.injectResolutionRule { session =>
      val mode = session.conf.get(
        DeltaSQLConfV2.V2_ENABLE_MODE.key,
        DeltaSQLConfV2.V2_ENABLE_MODE.defaultValueString)

      if ("STRICT".equals(mode)) {
        // V2 mode: inject conversion rule to convert SparkTable to DeltaTableV2
        new ConvertSparkTableToDeltaTableV2(session)
      } else {
        // NONE mode (default): no conversion needed, catalog returns DeltaTableV2 directly
        new NoOpRule()
      }
    }
    true
  }

  /**
   * NoOpRule for binary compatibility with Delta 3.3.0
   * This class must remain here to satisfy MiMa checks
   */
  class NoOpRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan
  }
}
