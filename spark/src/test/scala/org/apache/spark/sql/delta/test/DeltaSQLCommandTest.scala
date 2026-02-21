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

package org.apache.spark.sql.delta.test

import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.sql.DeltaSparkSessionExtension

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * A trait for tests that are testing a fully set up SparkSession with all of Delta's requirements,
 * such as the configuration of the DeltaCatalog and the addition of all Delta extensions.
 *
 * The V2 connector enable mode can be controlled via the `DELTA_V2_ENABLE_MODE` environment
 * variable. Valid values are "NONE", "AUTO", and "STRICT". When not set, the default from
 * [[DeltaSQLConf.V2_ENABLE_MODE]] ("AUTO") is used. This allows different CI workflows to
 * run tests against different connector modes without code changes.
 */
trait DeltaSQLCommandTest extends SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
        classOf[DeltaCatalog].getName)
    sys.env.get("DELTA_V2_ENABLE_MODE").foreach { mode =>
      conf.set(DeltaSQLConf.V2_ENABLE_MODE.key, mode)
    }
    conf
  }
}
