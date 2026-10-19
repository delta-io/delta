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

import org.apache.spark.sql.delta.actions.AddFile
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

  private val sparkCatalogKey = "spark.sql.catalog.spark_catalog"
  private val deltaCatalogV1ClassName = "org.apache.spark.sql.delta.catalog.DeltaCatalogV1"

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set(sparkCatalogKey, deltaCatalogV1ClassName)

  override def beforeEach(): Unit = {
    super.beforeEach()
    DeltaV2TableManagerCache.clearCache()
  }

  override def afterEach(): Unit = {
    DeltaV2TableManagerCache.clearCache()
    super.afterEach()
  }

  private def forPathAndCatalogManagers(
      path: String)(testFn: (DeltaV2TableManagerImpl, Option[CatalogTable]) => Unit): Unit = {
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
          testFn(manager, catalogTableOpt)
        }
    }
  }

  test("snapshotManager loads table history through path and catalog managers") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 1, 1, 1).write.format("delta").save(path)
      spark.range(1, 2, 1, 1).write.format("delta").mode("append").save(path)
      spark.range(2, 3, 1, 1).write.format("delta").mode("append").save(path)

      forPathAndCatalogManagers(path) { (manager, catalogTableOpt) =>
        val kernelContext = manager.kernelContext
        val atVersionZeroManager = manager.snapshotManager(catalogTableOpt)
        val atVersionOneManager = manager.snapshotManager(catalogTableOpt)
        val latestManager = manager.snapshotManager(catalogTableOpt)

        assert(atVersionZeroManager eq atVersionOneManager)
        assert(atVersionOneManager eq latestManager)
        assert(manager.kernelContext eq kernelContext)

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

  test("getChangesIterator yields one DeltaSingleCommit per version in range") {
    withSQLConf(
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "false",
        DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "false") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        spark.range(0, 2, 1, 2).write.format("delta").save(path)
        spark.range(2, 4, 1, 2).write.format("delta").mode("append").save(path)
        spark.range(4, 6, 1, 2).write.format("delta").mode("append").save(path)

        forPathAndCatalogManagers(path) { (manager, catalogTableOpt) =>
          val ranges = Seq(
            (0L, Some(1L), List(0L, 1L)),
            (1L, Some(1L), List(1L)),
            (1L, None, List(1L, 2L)))
          ranges.foreach { case (startVersion, endVersion, expectedVersions) =>
            val changes = manager.getChangesIterator(startVersion, endVersion, catalogTableOpt)
            try {
              val versions = changes.map { commit =>
                try {
                  commit.getActionsIterator().processAndClose { iterator =>
                    assert(iterator.count(_.isInstanceOf[AddFile]) === 2)
                  }
                  commit.version
                } finally {
                  commit.close()
                }
              }.toList
              assert(versions === expectedVersions)
            } finally {
              changes.close()
            }
          }
        }
      }
    }
  }

  test("getChangesIterator rejects failOnDataLoss = true before reading the table") {
    withTempDir { dir =>
      forPathAndCatalogManagers(dir.getCanonicalPath) { (manager, catalogTableOpt) =>
        Seq(None, Some(1L)).foreach { endVersion =>
          val error = intercept[AssertionError] {
            manager.getChangesIterator(0L, endVersion, catalogTableOpt, failOnDataLoss = true)
          }
          assert(error.getMessage ===
            "getChangesIterator does not support failOnDataLoss = true")
        }
      }
    }
  }

  test("cached composite reuses the table manager and KernelContext") {
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
        assert(first.kernelContext eq second.kernelContext)
      }
    }
  }
}
