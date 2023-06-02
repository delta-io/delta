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

package org.apache.spark.sql.delta.files

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructType}

class TransactionalWriteSuite extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {

  test("writing out an empty dataframe produces no AddFiles") {
    withTempDir { dir =>
      spark.range(100).write.format("delta").save(dir.getCanonicalPath)

      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val schema = new StructType().add("id", StringType)
      val emptyDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      assert(log.startTransaction().writeFiles(emptyDf).isEmpty)
    }
  }
}
