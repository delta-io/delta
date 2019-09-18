/*
 * Copyright 2019 Databricks, Inc.
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

import io.delta.sql.parser.DeltaSqlParser

import org.apache.spark.sql.SparkSessionExtensions

/**
 * An extension for Spark SQL to support Delta's SQL commands. This one can be enabled by setting
 * `spark.sql.extensions` to `io.delta.sql.DeltaSparkSessionExtension` before creating your
 * `SparkSession`.
 */
class DeltaSparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (session, parser) =>
      new DeltaSqlParser(parser)
    }
  }
}
