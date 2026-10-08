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


package org.apache.spark.sql.delta.test

import org.apache.spark.sql.delta.catalog.DeltaCatalog
import io.delta.sql.DeltaSparkSessionExtension

import org.apache.spark.SparkConf
import org.apache.spark.sql.SessionQueryTest
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

/**
 * A SessionQueryTest with Delta's session extension and catalog configured.
 */
trait DeltaSessionQueryTest extends SessionQueryTest {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
        classOf[DeltaCatalog].getName)
  }
}
