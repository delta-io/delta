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

import org.apache.spark.sql.delta.optimizer.RangePartitionIdRewrite
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.stats.PrepareDeltaScan
import io.delta.sql.parser.DeltaSqlParser

import org.apache.spark.sql.SparkSessionExtensions
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
    extensions.injectParser { (session, parser) =>
      new DeltaSqlParser(parser)
    }
    extensions.injectResolutionRule { session =>
      new PreprocessTimeTravel(session)
    }
    extensions.injectResolutionRule { session =>
      // To ensure the parquet field id reader is turned on, these fields are required to support
      // id column mapping mode for Delta.
      // Spark has the read flag default off, so we have to turn it on manually for Delta.
      session.sessionState.conf.setConf(SQLConf.PARQUET_FIELD_ID_READ_ENABLED, true)
      session.sessionState.conf.setConf(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED, true)
      new DeltaAnalysis(session)
    }
    extensions.injectCheckRule { session =>
      new DeltaUnsupportedOperationsCheck(session)
    }
    // Rule for rewriting the place holder for range_partition_id to manually construct the
    // `RangePartitioner` (which requires an RDD to be sampled in order to determine
    // range partition boundaries)
    extensions.injectOptimizerRule { session =>
      new RangePartitionIdRewrite(session)
    }
    extensions.injectPostHocResolutionRule { session =>
      new PreprocessTableUpdate(session.sessionState.conf)
    }
    extensions.injectPostHocResolutionRule { session =>
      new PreprocessTableMerge(session.sessionState.conf)
    }
    extensions.injectPostHocResolutionRule { session =>
      new PreprocessTableDelete(session.sessionState.conf)
    }
    // We don't use `injectOptimizerRule` here as we won't want to apply further optimizations after
    // `PrepareDeltaScan`.
    // For example, `ConstantFolding` will break unit tests in `OptimizeGeneratedColumnSuite`.
    extensions.injectPreCBORule { session =>
      new PrepareDeltaScan(session)
    }

    DeltaTableValueFunctions.supportedFnNames.foreach { fnName =>
      extensions.injectTableFunction(
        DeltaTableValueFunctions.getTableValueFunctionInjection(fnName))
    }
  }
}
