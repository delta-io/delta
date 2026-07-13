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

package org.apache.spark.sql.delta.v2.interop

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import io.delta.kernel.TableManager
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.SnapshotImpl

class KernelSnapshotUtilsSuite extends DeltaSQLCommandTest {

  // scalastyle:off deltahadoopconfiguration
  private def engine: Engine = DefaultEngine.create(spark.sessionState.newHadoopConf())
  // scalastyle:on deltahadoopconfiguration

  private def kernelSnapshotFor(path: String, engine: Engine): SnapshotImpl =
    TableManager.loadSnapshot(path).build(engine).asInstanceOf[SnapshotImpl]

  private def allFilesFor(path: String): (Array[AddFile], Array[AddFile]) = {
    val kernelEngine = engine
    val kernelSnapshot = kernelSnapshotFor(path, kernelEngine)
    val kernelFiles = KernelSnapshotUtils
      .buildAllFiles(kernelSnapshot, spark, kernelEngine)
      .collect()
    val v1Files = DeltaLog.forTable(spark, path).snapshot.allFiles.collect()
    (kernelFiles, v1Files)
  }

  private def assertSameAddFilesAsV1(
      kernelFiles: Array[AddFile],
      v1Files: Array[AddFile]): Unit = {
    val kernelFilesByPath = kernelFiles.map(file => file.path -> file).toMap
    val v1FilesByPath = v1Files.map(file => file.path -> file).toMap

    assert(kernelFilesByPath.size === kernelFiles.length)
    assert(v1FilesByPath.size === v1Files.length)
    assert(kernelFilesByPath.keySet === v1FilesByPath.keySet)

    v1FilesByPath.foreach { case (path, v1File) =>
      val kernelFile = kernelFilesByPath(path)
      withClue(s"AddFile mismatch for $path: ") {
        assert(kernelFile.partitionValues === v1File.partitionValues)
        assert(kernelFile.size === v1File.size)
        assert(kernelFile.modificationTime === v1File.modificationTime)
        assert(kernelFile.dataChange === v1File.dataChange)
        assert(kernelFile.stats === v1File.stats)
        assert(kernelFile.deletionVector === v1File.deletionVector)
        assert(kernelFile.baseRowId === v1File.baseRowId)
        assert(kernelFile.defaultRowCommitVersion === v1File.defaultRowCommitVersion)
      }
    }
  }

  test("buildAllFiles returns V1 file metadata and preserves stats") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      spark.range(3).write.format("delta").save(path)
      val (kernelFiles, v1Files) = allFilesFor(path)

      assertSameAddFilesAsV1(kernelFiles, v1Files)
      assert(kernelFiles.forall(file => !file.dataChange))
      assert(kernelFiles.map(_.stats).forall(stat => stat != null && stat.nonEmpty))
    }
  }

  test("buildAllFiles preserves partition values") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      spark.range(4)
        .selectExpr("id", "CAST(id % 2 AS INT) AS part")
        .write
        .partitionBy("part")
        .format("delta")
        .save(path)
      val (kernelFiles, v1Files) = allFilesFor(path)

      assert(v1Files.map(_.partitionValues).toSet === Set(
        Map("part" -> "0"),
        Map("part" -> "1")))
      assertSameAddFilesAsV1(kernelFiles, v1Files)
    }
  }

  test("buildAllFiles preserves null partition values") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      spark.range(2)
        .selectExpr(
          "id",
          "IF(id = 0, CAST(NULL AS INT), CAST(id AS INT)) AS part")
        .write
        .partitionBy("part")
        .format("delta")
        .save(path)
      val (kernelFiles, v1Files) = allFilesFor(path)

      assert(v1Files.map(_.partitionValues).toSet === Set(
        Map("part" -> null),
        Map("part" -> "1")))
      assertSameAddFilesAsV1(kernelFiles, v1Files)
    }
  }

  test("buildAllFiles preserves deletion vectors and row tracking fields") {
    withSQLConf(
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true",
        DeltaConfigs.ROW_TRACKING_ENABLED.defaultTablePropertyKey -> "true",
        DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> "true") {
      withTempDir { dir =>
        val path = dir.getAbsolutePath
        spark.range(10).repartition(1).write.format("delta").save(path)
        sql(s"DELETE FROM delta.`$path` WHERE id = 0")
        val (kernelFiles, v1Files) = allFilesFor(path)

        assert(v1Files.exists(_.deletionVector != null))
        assert(v1Files.exists(_.baseRowId.nonEmpty))
        assert(v1Files.exists(_.defaultRowCommitVersion.nonEmpty))
        assertSameAddFilesAsV1(kernelFiles, v1Files)
      }
    }
  }
}
