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

import java.io.File
import java.util.Collections

import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

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

  private def forPathAndCatalogManagers(
      path: String)(testFn: DeltaV2TableManagerImpl => Unit): Unit = {
    val catalogTable = CatalogTable(
      identifier = TableIdentifier("test_table"),
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat.empty.copy(locationUri = Some(new File(path).toURI)),
      schema = new StructType())
    Seq("path-based" -> None, "catalog-backed" -> Some(catalogTable)).foreach {
      case (label, catalogTableOpt) =>
        DeltaV2TableManagerCache.clearCache()
        withClue(s"$label manager: ") {
          val manager = DeltaV2TableManagerCache
            .forTable(spark, path, Collections.emptyMap(), catalogTableOpt)
            .asInstanceOf[DeltaV2TableManagerImpl]
          assert(manager.initialCatalogTableOpt === catalogTableOpt)
          testFn(manager)
        }
    }
  }

  test("snapshotManager loads table history through path and catalog managers") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 1, 1, 1).write.format("delta").save(path)
      spark.range(1, 2, 1, 1).write.format("delta").mode("append").save(path)
      spark.range(2, 3, 1, 1).write.format("delta").mode("append").save(path)

      forPathAndCatalogManagers(path) { manager =>
        val kernelEngine = manager.kernelContext.getDefaultEngine()
        val atVersionZeroManager = manager.snapshotManager
        val atVersionOneManager = manager.snapshotManager
        val latestManager = manager.snapshotManager

        assert(atVersionZeroManager ne atVersionOneManager)
        assert(atVersionOneManager ne latestManager)
        assert(manager.kernelContext.getDefaultEngine() eq kernelEngine)

        val atVersionZero = atVersionZeroManager.loadSnapshotAt(0)
        assert(atVersionZero.version == 0)
        assert(atVersionZero.allFiles.count() == 1)

        val atVersionOne = atVersionOneManager.loadSnapshotAt(1)
        assert(atVersionOne.version == 1)
        assert(atVersionOne.allFiles.count() == 2)

        val latest = latestManager.loadLatestSnapshot()
        assert(latest.version == 2)
        assert(latest.allFiles.count() == 3)
      }
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
