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

// scalastyle:off import.ordering.noEmptyLine
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.test.SharedSparkSession

class DeltaTableUtilsSuite extends SharedSparkSession {

  test("findDeltaTableRoot correctly combines paths") {
    withTempDir { dir =>
      sql(s"CREATE TABLE myTable (id INT) USING DELTA LOCATION '${dir.getAbsolutePath}'")
      val path = new Path(s"file://${dir.getAbsolutePath}")
      assert(DeltaTableUtils.findDeltaTableRoot(spark, path).contains(path))
    }
  }

  test("safeConcatPaths") {
    val basePath = new Path("s3://my-bucket/subfolder")
    val basePathEmpty = new Path("s3://my-bucket")
    assert(DeltaTableUtils.safeConcatPaths(basePath, "_delta_log") ==
      new Path("s3://my-bucket/subfolder/_delta_log"))
    assert(DeltaTableUtils.safeConcatPaths(basePathEmpty, "_delta_log") ==
      new Path("s3://my-bucket/_delta_log"))
  }
}
