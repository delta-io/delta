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
package io.delta.spark.internal.v2.tablemanager

import java.util.Collections

import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class DeltaV2TableManagerImplSuite
    extends QueryTest
    with SharedSparkSession {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", classOf[DeltaCatalog].getName)

  override def beforeEach(): Unit = {
    super.beforeEach()
    DeltaV2TableManagerCache.clearCache()
  }

  override def afterEach(): Unit = {
    DeltaV2TableManagerCache.clearCache()
    super.afterEach()
  }

  test("snapshotManager reuses an uncached delegate and the shared Kernel Engine") {
    withTempDir { dir =>
      spark.range(1).write.format("delta").save(dir.getCanonicalPath)
      val impl = DeltaV2TableManagerCache
        .forTable(spark, dir.getCanonicalPath, Collections.emptyMap())
        .asInstanceOf[DeltaV2TableManagerImpl]

      val kernelEngine = impl.kernelContext.getDefaultEngine()
      val first = impl.snapshotManager
      val second = impl.snapshotManager

      assert(first eq second)
      assert(impl.kernelContext.getDefaultEngine() eq kernelEngine)
      assert(first.loadLatestSnapshot().version == 0)
      assert(second.loadLatestSnapshot().version == 0)
    }
  }

  test("cached composite reuses the table manager and Kernel Engine") {
    withSQLConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key -> "1000") {
      withTempDir { dir =>
        val first = DeltaV2TableManagerCache
          .forTable(
            spark,
            dir.getCanonicalPath,
            Collections.emptyMap())
          .asInstanceOf[DeltaV2TableManagerImpl]
        val second = DeltaV2TableManagerCache
          .forTable(
            spark,
            dir.getCanonicalPath,
            Collections.emptyMap())
          .asInstanceOf[DeltaV2TableManagerImpl]

        assert(first eq second)
        assert(first.kernelContext.getDefaultEngine() eq second.kernelContext.getDefaultEngine())
      }
    }
  }
}
