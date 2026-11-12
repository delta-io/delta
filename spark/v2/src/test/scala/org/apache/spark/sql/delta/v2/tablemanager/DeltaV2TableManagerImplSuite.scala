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
package org.apache.spark.sql.delta.v2.tablemanager

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.{Collections, EnumSet, Optional}

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog}
import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, Protocol}
import org.apache.spark.sql.delta.sources.{DeltaSource, DeltaSQLConf}
import org.apache.spark.sql.delta.v2.exception.VersionNotFoundException
import org.apache.spark.sql.delta.v2.interop.DeltaV2QueryContext
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager.GetTableChangesParam
import org.apache.spark.sql.delta.v2.utils.StreamingHelper
import io.delta.kernel.exceptions.{
  InvalidTableException => KernelInvalidTableException,
  StartVersionNotFoundException => KernelStartVersionNotFoundException
}
import io.delta.kernel.internal.DeltaLogActionUtils.{DeltaAction => KernelDeltaAction}

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

  test("query context APIs load table history through path and catalog managers") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 1, 1, 1).write.format("delta").save(path)
      spark.range(1, 2, 1, 1).write.format("delta").mode("append").save(path)
      spark.range(2, 3, 1, 1).write.format("delta").mode("append").save(path)

      forPathAndCatalogManagers(path) { (manager, catalogTableOpt) =>
        val kernelContext = manager.kernelContext
        val queryContext = DeltaV2QueryContext(catalogTableOpt)
        val kernelEngine = kernelContext.getDefaultEngine()
        val atVersionZeroManager = manager.snapshotManager(catalogTableOpt)
        val atVersionOneManager = manager.snapshotManager(catalogTableOpt)
        val latestManager = manager.snapshotManager(catalogTableOpt)

        assert(atVersionZeroManager eq atVersionOneManager)
        assert(atVersionOneManager eq latestManager)
        assert(manager.kernelContext eq kernelContext)

        val atVersionZero = manager.loadSnapshotAt(0, queryContext)
        assert(atVersionZero.version == 0)
        assert(atVersionZero.allFiles.count() == 1)

        val atVersionOne = manager.loadSnapshotAt(1, queryContext)
        assert(atVersionOne.version == 1)
        assert(atVersionOne.allFiles.count() == 2)

        val latest = manager.loadLatestSnapshot(queryContext)
        assert(latest.version == 2)
        assert(latest.allFiles.count() == 3)

        val activeCommit = manager.getActiveCommitAtTime(
          Long.MaxValue,
          canReturnLastCommit = true,
          mustBeRecreatable = true,
          canReturnEarliestCommit = false,
          queryContext = queryContext)
        assert(activeCommit.getVersion == 2)

        manager.checkVersionExists(1, true, false, queryContext)
        intercept[VersionNotFoundException] {
          manager.checkVersionExists(3, true, false, queryContext)
        }
        manager.checkVersionExists(3, true, true, queryContext)

        val changes = manager.getTableChanges(
          kernelEngine,
          GetTableChangesParam(1, Some(1L)),
          queryContext)
        val commits = changes.getCommitActions(
          kernelEngine, EnumSet.allOf(classOf[KernelDeltaAction]))
        try {
          assert(commits.hasNext)
          val commit = commits.next()
          try {
            assert(commit.getVersion == 1)
          } finally {
            commit.close()
          }
          assert(!commits.hasNext)
        } finally {
          commits.close()
        }
      }
    }
  }

  test("query context APIs reject null context") {
    withTempDir { dir =>
      val manager = DeltaV2TableManagerCache.forTable(
        spark, dir.getCanonicalPath, Collections.emptyMap())
      val operations = Seq[() => Any](
        () => manager.getSingleCommitIterator(0, None, null),
        () => manager.loadLatestSnapshot(null),
        () => manager.loadSnapshotAt(0, null),
        () => manager.getActiveCommitAtTime(0, false, false, false, null),
        () => manager.checkVersionExists(0, false, false, null),
        () => manager.getTableChanges(null, GetTableChangesParam(0, None), null))
      operations.foreach { operation =>
        val error = intercept[NullPointerException](operation())
        assert(error.getMessage == "queryContext is null")
      }
    }
  }

  Seq((true, true), (true, false), (false, true), (false, false)).foreach {
    case (commitRangeValidation, failOnDataLoss) =>
      test("getTableChanges applies commit range flags " +
          s"(commitRangeValidation=$commitRangeValidation, failOnDataLoss=$failOnDataLoss)") {
        withSQLConf(
            DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "false",
            DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "false") {
          withTempDir { dir =>
            val path = dir.getCanonicalPath
            spark.range(0, 1, 1, 1).write.format("delta").save(path)
            spark.range(1, 2, 1, 1).write.format("delta").mode("append").save(path)
            spark.range(2, 3, 1, 1).write.format("delta").mode("append").save(path)
            Files.delete(new File(dir, "_delta_log/00000000000000000001.json").toPath)

            forPathAndCatalogManagers(path) { (manager, catalogTableOpt) =>
              val kernelEngine = manager.kernelContext.getDefaultEngine()
              val queryContext = DeltaV2QueryContext(catalogTableOpt)
              Seq(None, Some(2L)).foreach { endVersion =>
                val params = GetTableChangesParam(
                  0L, endVersion, commitRangeValidation, failOnDataLoss)
                if (commitRangeValidation) {
                  val error = intercept[KernelInvalidTableException] {
                    manager.getTableChanges(kernelEngine, params, queryContext)
                  }
                  assert(error.getMessage.contains("versions are not contiguous"))
                } else {
                  val kernelCommitRange =
                    manager.getTableChanges(kernelEngine, params, queryContext)
                  try {
                    val kernelCommitFiles = kernelCommitRange.getCommitFiles()
                    try {
                      assert(kernelCommitFiles.hasNext)
                      assert(kernelCommitFiles.next().getVersion === 0L)
                      if (failOnDataLoss) {
                        val error = intercept[KernelInvalidTableException] {
                          kernelCommitFiles.hasNext
                        }
                        assert(error.getMessage.contains(path))
                      } else {
                        assert(kernelCommitFiles.hasNext)
                        assert(kernelCommitFiles.next().getVersion === 2L)
                        assert(!kernelCommitFiles.hasNext)
                      }
                    } finally {
                      kernelCommitFiles.close()
                    }
                  } finally {
                    kernelCommitRange match {
                      case closeable: AutoCloseable => closeable.close()
                      case _ =>
                    }
                  }
                }
              }
            }
          }
        }
      }
  }

  test("getSingleCommitIterator yields one DeltaSingleCommit per version in range") {
    withSQLConf(
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "false",
        DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "false",
        DeltaSQLConf.LOG_SIZE_IN_MEMORY_THRESHOLD.key -> "1") {
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
          for {
            (startVersion, endVersion, expectedVersions) <- ranges
            failOnDataLoss <- Seq(false, true)
          } {
            val changes = manager.getSingleCommitIterator(
              startVersion, endVersion, DeltaV2QueryContext(catalogTableOpt), failOnDataLoss)
            try {
              val versions = changes.map { commit =>
                val actions = DeltaSource.createRewindableActionIterator(spark, commit)
                actions.processAndClose { iterator =>
                  assert(iterator.count(_.isInstanceOf[AddFile]) === 2)
                }
                commit.version
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

  Seq((0L, false), (0L, true), (1L, false), (1L, true)).foreach {
    case (missingVersion, failOnDataLoss) =>
      test("getSingleCommitIterator handles missing versions lazily " +
          s"(missingVersion=$missingVersion, failOnDataLoss=$failOnDataLoss)") {
        withSQLConf(
            DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "false",
            DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "false") {
          withTempDir { dir =>
            val path = dir.getCanonicalPath
            spark.range(1).write.format("delta").save(path)
            spark.range(1, 2).write.format("delta").mode("append").save(path)
            spark.range(2, 3).write.format("delta").mode("append").save(path)
            Files.delete(new File(dir, f"_delta_log/$missingVersion%020d.json").toPath)
            forPathAndCatalogManagers(path) { (manager, catalogTableOpt) =>
              Seq(None, Some(2L)).foreach { endVersion =>
                val commits = manager.getSingleCommitIterator(
                  0L, endVersion, DeltaV2QueryContext(catalogTableOpt), failOnDataLoss)
                try {
                  if (failOnDataLoss) {
                    if (missingVersion == 1L) {
                      assert(commits.next().version === 0L)
                    }
                    if (missingVersion == 0L) {
                      intercept[KernelStartVersionNotFoundException] { commits.hasNext }
                    } else {
                      val error = intercept[KernelInvalidTableException] { commits.hasNext }
                      assert(error.getMessage.contains("version 1"))
                    }
                  } else {
                    val expected = (0L to 2L).filter(_ != missingVersion).toList
                    assert(commits.map(_.version).toList === expected)
                  }
                } finally {
                  commits.close()
                }
              }
            }
          }
        }
      }
  }

  Seq((false, false), (true, true), (false, true)).foreach {
    case (enableICTInitially, enableICTLater) =>
      test("V1 and V2 commit timestamps agree across ICT enablement " +
          s"(enableICTInitially=$enableICTInitially, enableICTLater=$enableICTLater)") {
        withSQLConf(
            DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "false",
            DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "false") {
          withTempDir { dir =>
            val path = dir.getCanonicalPath
            spark.range(1).write.format("delta")
              .option(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key, enableICTInitially.toString)
              .save(path)
            if (!enableICTInitially && enableICTLater) {
              sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES " +
                s"('${DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key}' = 'true')")
            }
            spark.range(1, 2).write.format("delta").mode("append").save(path)
            spark.range(2, 3).write.format("delta").mode("append").save(path)

            val deltaLog = DeltaLog.forTable(spark, path)
            val hadoopConf = deltaLog.newDeltaHadoopConf()
            val fileTimestamp = 3000L
            // Equal mtimes expose accidental monotonization and differ from all persisted ICTs.
            deltaLog.getChangeLogFiles(startVersion = 0).foreach { case (_, status) =>
              status.getPath.getFileSystem(hadoopConf).setTimes(status.getPath, fileTimestamp, -1L)
            }
            val expected = deltaLog.getChanges(startVersion = 0).map { case (version, actions) =>
              val ict = actions.collectFirst { case info: CommitInfo => info }
                .flatMap(_.inCommitTimestamp)
              val expectsICT = enableICTInitially || (enableICTLater && version > 0)
              assert(ict.isDefined === expectsICT, s"version $version")
              version -> (if (expectsICT) ict.get else fileTimestamp)
            }.toList
            val v1Commits = deltaLog.getSingleCommitIterator(startVersion = 0)
              .map(commit => commit.version -> commit.timestamp).toList
            assert(v1Commits === expected)

            forPathAndCatalogManagers(path) { (manager, catalogTableOpt) =>
              val v2Commits = manager.getSingleCommitIterator(
                0L, None, DeltaV2QueryContext(catalogTableOpt))
                .processAndClose(_.map(commit => commit.version -> commit.timestamp).toList)
              assert(v2Commits === expected)
            }
          }
        }
      }
  }

  test("metadata and protocol collectors reject duplicate actions") {
    Seq("metaData", "protocol").foreach { actionName =>
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        spark.range(1).write.format("delta").save(path)
        val snapshot = DeltaLog.forTable(spark, path).update()
        val actions: Seq[Action] = if (actionName == "metaData") {
          Seq(
            snapshot.metadata.copy(description = "first metadata"),
            snapshot.metadata.copy(description = "second metadata"))
        } else {
          Seq(Protocol(1, 1), Protocol(1, 2))
        }
        Files.writeString(
          new File(dir, "_delta_log/00000000000000000001.json").toPath,
          actions.map(_.json).mkString("", "\n", "\n"),
          UTF_8)

        forPathAndCatalogManagers(path) { (manager, catalogTableOpt) =>
          val queryContext = DeltaV2QueryContext(catalogTableOpt)
          val error = intercept[IllegalArgumentException] {
            if (actionName == "metaData") {
              StreamingHelper.collectMetadataActions(
                1L, Optional.of(Long.box(1L)), manager, queryContext, true)
            } else {
              StreamingHelper.collectProtocolActions(
                1L, Optional.of(Long.box(1L)), manager, queryContext, true)
            }
          }
          assert(error.getMessage ===
            s"Should not encounter two $actionName actions in the same commit of version 1")
        }
      }
    }
  }

  test("collectors parse log actions without converting metadata schemas through Kernel") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(1).write.format("delta").save(path)
      val snapshot = DeltaLog.forTable(spark, path).update()
      val metadata = snapshot.metadata.copy(
        schemaString = """{"type":"struct","fields":[""" +
          """{"name":"value","type":"UnsupportedType","nullable":true,"metadata":{}}]}""")
      Files.writeString(
        new File(dir, "_delta_log/00000000000000000001.json").toPath,
        Seq(snapshot.protocol, metadata).map(_.json).mkString("", "\n", "\n"), UTF_8)
      // Keep the latest snapshot valid so collection inspects the historical commit.
      Files.writeString(
        new File(dir, "_delta_log/00000000000000000002.json").toPath,
        Seq(snapshot.metadata, snapshot.protocol).map(_.json).mkString("", "\n", "\n"), UTF_8)

      forPathAndCatalogManagers(path) { (manager, catalogTableOpt) =>
        val queryContext = DeltaV2QueryContext(catalogTableOpt)
        val metadataActions = StreamingHelper.collectMetadataActions(
          1L, Optional.of(Long.box(1L)), manager, queryContext, true)
        assert(metadataActions.size() === 1)
        assert(metadataActions.get(1L).schemaString === metadata.schemaString)
        val protocols = StreamingHelper.collectProtocolActions(
          1L, Optional.of(Long.box(1L)), manager, queryContext, true)
        assert(protocols.size() === 1)
        assert(protocols.get(1L) === snapshot.protocol)
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
