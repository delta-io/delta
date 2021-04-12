/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import org.apache.spark.sql.delta._
import io.delta.sql.parser.DeltaSqlParser

import org.apache.spark.sql.SparkSessionExtensions
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
      new DeltaAnalysis(session, session.sessionState.conf)
    }
    extensions.injectCheckRule { session =>
      new DeltaUnsupportedOperationsCheck(session)
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
    extensions.injectOptimizerRule { session =>
      new ActiveOptimisticTransactionRule(session)
    }
  }
}
