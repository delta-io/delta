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

package org.apache.spark.sql.delta

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.internal.SQLConf

class DeltaRestartSessionSuite extends SparkFunSuite {

  test("restart Spark session should work") {
    withTempDir { dir =>
      var spark = SparkSession.builder().master("local[2]")
        .config(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
        .getOrCreate()
      try {
        val path = dir.getCanonicalPath
        spark.range(10).write.format("delta").mode("overwrite").save(path)
        spark.read.format("delta").load(path).count()

        spark.stop()
        spark = SparkSession.builder().master("local[2]")
          .config(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
          .getOrCreate()
        spark.range(10).write.format("delta").mode("overwrite").save(path)
        spark.read.format("delta").load(path).count()
      }
      finally {
        spark.stop()
      }
    }
  }
}
