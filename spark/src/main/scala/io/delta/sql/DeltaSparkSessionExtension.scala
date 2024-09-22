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

package io.delta.sql

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.optimizer.RangePartitionIdRewrite
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.PrepareDeltaScan
import io.delta.sql.parser.DeltaSqlParser

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.PreprocessTimeTravel
import org.apache.spark.sql.internal.SQLConf

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
class DeltaSparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (_, parser) =>
      new DeltaSqlParser(parser)
    }
    extensions.injectResolutionRule { session =>
      ResolveDeltaPathTable(session)
    }
    extensions.injectResolutionRule { session =>
      PreprocessTimeTravel(session)
    }
    extensions.injectResolutionRule { session =>
      // To ensure the parquet field id reader is turned on, these fields are required to support
      // id column mapping mode for Delta.
      // Spark has the read flag default off, so we have to turn it on manually for Delta.
      session.sessionState.conf.setConf(SQLConf.PARQUET_FIELD_ID_READ_ENABLED, true)
      session.sessionState.conf.setConf(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED, true)
      new DeltaAnalysis(session)
    }
    // [SPARK-45383] Spark CheckAnalysis rule misses a case for RelationTimeTravel, and so a
    // non-existent table throws an internal spark error instead of the expected AnalysisException.
    extensions.injectCheckRule { session =>
      new CheckUnresolvedRelationTimeTravel(session)
    }
    extensions.injectCheckRule { session =>
      DeltaUnsupportedOperationsCheck(session)
    }
    // Rule for rewriting the place holder for range_partition_id to manually construct the
    // `RangePartitioner` (which requires an RDD to be sampled in order to determine
    // range partition boundaries)
    extensions.injectOptimizerRule { session =>
      new RangePartitionIdRewrite(session)
    }
    extensions.injectPostHocResolutionRule { session =>
      PreprocessTableUpdate(session.sessionState.conf)
    }
    extensions.injectPostHocResolutionRule { session =>
      PreprocessTableMerge(session.sessionState.conf)
    }
    extensions.injectPostHocResolutionRule { session =>
      PreprocessTableDelete(session.sessionState.conf)
    }
    // Resolve new UpCast expressions that might have been introduced by [[PreprocessTableUpdate]]
    // and [[PreprocessTableMerge]].
    extensions.injectPostHocResolutionRule { session =>
      PostHocResolveUpCast(session)
    }

    extensions.injectPlanNormalizationRule { _ => GenerateRowIDs }

    extensions.injectPreCBORule { session =>
      new PrepareDeltaScan(session)
    }
    // Fold constants that may have been introduced by PrepareDeltaScan. This is only useful with
    // Spark 3.5 as later versions apply constant folding after pre-CBO rules.
    extensions.injectPreCBORule { _ => ConstantFolding }

    // Add skip row column and filter.
    extensions.injectPlannerStrategy(PreprocessTableWithDVsStrategy)

    // Tries to load PrepareDeltaSharingScan class with class reflection, when delta-sharing-spark
    // 3.1+ package is installed, this will be loaded and delta sharing batch queries with
    // DeltaSharingFileIndex will be handled by the rule.
    // When the package is not installed or upon any other issues, it should do nothing and not
    // affect all the existing rules.
    try {
      // scalastyle:off classforname
      val constructor = Class.forName("io.delta.sharing.spark.PrepareDeltaSharingScan")
        .getConstructor(classOf[org.apache.spark.sql.SparkSession])
      // scalastyle:on classforname
      extensions.injectPreCBORule { session =>
        try {
          // Inject the PrepareDeltaSharingScan rule if enabled, otherwise, inject the no op
          // rule. It can be disabled if there are any issues so all existing rules are not blocked.
          if (
            session.conf.get(DeltaSQLConf.DELTA_SHARING_ENABLE_DELTA_FORMAT_BATCH.key) == "true"
          ) {
            constructor.newInstance(session).asInstanceOf[Rule[LogicalPlan]]
          } else {
            new NoOpRule
          }
        } catch {
          // Inject a no op rule which doesn't apply any changes to the logical plan.
          case NonFatal(_) => new NoOpRule
        }
      }
    } catch {
      case NonFatal(_) => // Do nothing
    }

    DeltaTableValueFunctions.supportedFnNames.foreach { fnName =>
      extensions.injectTableFunction(
        DeltaTableValueFunctions.getTableValueFunctionInjection(fnName))
    }
  }

  /**
   * An no op rule which doesn't apply any changes to the LogicalPlan. Used to be injected upon
   * exceptions.
   */
  class NoOpRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan
  }
}
