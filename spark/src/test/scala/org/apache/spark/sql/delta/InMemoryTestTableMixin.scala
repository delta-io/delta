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

package org.apache.spark.sql.delta

import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.catalog.InMemoryDeltaCatalog
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Mixin trait that configures the session catalog to use [[InMemoryDeltaCatalog]],
 * routing DML operations through Spark's V2 execution path via [[InMemorySparkTable]].
 */
trait InMemoryTestTableMixin extends SharedSparkSession {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.spark_catalog", classOf[InMemoryDeltaCatalog].getName)
}
