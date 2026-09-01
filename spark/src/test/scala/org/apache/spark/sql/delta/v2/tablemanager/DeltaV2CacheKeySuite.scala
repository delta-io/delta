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

package org.apache.spark.sql.delta.v2.tablemanager

import java.io.File

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class DeltaV2CacheKeySuite
    extends QueryTest
    with SharedSparkSession
{

  test("produces a _delta_log path from the data path") {
    withTempDir { dataPath =>
      val key = DeltaV2CacheKey.from(
        spark, dataPath.getCanonicalPath, Map.empty[String, String].asJava)

      assert(key.path.isAbsolute)
      assert(key.path.toString.endsWith("_delta_log"))
      assert(key.path.getParent.toString == dataPath.getCanonicalPath)
    }
  }

  test("same inputs produce equal keys") {
    withTempDir { dataPath =>
      val options = Map(
        "fs.test.option" -> "value", "reader.option" -> "ignored")
      val first = DeltaV2CacheKey.from(
        spark, dataPath.getCanonicalPath, options.asJava)
      val second = DeltaV2CacheKey.from(
        spark, dataPath.getCanonicalPath, options.asJava)
      assert(first === second)
    }
  }

  test("preserves path derivation for paths containing colons") {
    withTempDir { parent =>
      val dataPath = new File(parent, "table:with:colons")
      assert(dataPath.mkdirs())

      withSQLConf(
          DeltaSQLConf.DELTA_WORK_AROUND_COLONS_IN_HADOOP_PATHS.key -> "true") {
        val key = DeltaV2CacheKey.from(
          spark, dataPath.getCanonicalPath, Map.empty[String, String].asJava)
        assert(key.path.isAbsolute)
        assert(key.path.toString.endsWith("_delta_log"))
      }
    }
  }


  test("filters non-filesystem options and isolates filesystem credentials") {
    withTempDir { dataPath =>
      val path = dataPath.getCanonicalPath
      val firstOptions = Map(
        "fs.test.secret" -> "first",
        "dfs.test.endpoint" -> "endpoint",
        "reader.option" -> "ignored")
      val secondOptions = firstOptions.updated("fs.test.secret", "second")

      val first = DeltaV2CacheKey.from(spark, path, firstOptions.asJava)
      val second = DeltaV2CacheKey.from(spark, path, secondOptions.asJava)

      assert(first.sessionInvariantFsOptions === firstOptions.filter { case (key, _) =>
        key.startsWith("fs.") || key.startsWith("dfs.")
      })
      assert(!first.sessionInvariantFsOptions.contains("reader.option"))
      assert(first !== second)
    }
  }

  test("redacts filesystem option names and values from rendering") {
    withTempDir { dataPath =>
      val optionName = "fs.test.secret"
      val optionValue = "credential-value"
      val key = DeltaV2CacheKey.from(
        spark, dataPath.getCanonicalPath,
        Map(optionName -> optionValue).asJava)
      val rendered = key.toString

      assert(!rendered.contains(optionName))
      assert(!rendered.contains(optionValue))
      assert(rendered.contains("fsOptions=<redacted>"))
    }
  }
}
