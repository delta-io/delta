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

import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, NoMapping, Snapshot}
import org.apache.spark.sql.delta.actions.DomainMetadata
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.FileSizeHistogramUtils
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import io.delta.spark.internal.v2.kernel.KernelEngineFactory
import org.apache.hadoop.fs.Path
import io.delta.kernel.TableManager
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.{SnapshotImpl => KernelSnapshot}

/**
 * Tests for [[DeltaV2Snapshot]] focusing on the construction surface that is wired to the
 * underlying Kernel snapshot: `path` / `version`, the data members read from Kernel
 * (`metadata`, `protocol`, `allFiles`, `timestamp`, ...), and the V1 state-reconstruction members
 * that remain unsupported and throw [[UnsupportedOperationException]].
 */
class DeltaV2SnapshotSuite extends DeltaSQLCommandTest {

  // scalastyle:off deltahadoopconfiguration
  // No DeltaLog in this test (the snapshot is loaded via Kernel), so use the session Hadoop conf.
  private def engine: Engine =
    KernelEngineFactory.createDefaultEngine(spark.sessionState.newHadoopConf())
  // scalastyle:on deltahadoopconfiguration

  /** Load the latest snapshot of the Delta table at `path` via Kernel. */
  private def loadKernelSnapshot(path: String): KernelSnapshot =
    TableManager.loadSnapshot(path).build(engine).asInstanceOf[KernelSnapshot]

  test("path and version are derived from the wrapped Kernel snapshot") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(5).write.format("delta").save(path) // version 0

      val kernelSnapshot0 = loadKernelSnapshot(path)
      val snapshot0 = new DeltaV2Snapshot(kernelSnapshot0)
      assert(snapshot0.version === kernelSnapshot0.getVersion)
      assert(snapshot0.version === 0L)
      assert(snapshot0.path === new Path(kernelSnapshot0.getDataPath.toString))

      // version tracks the latest committed version after more commits.
      spark.range(1).write.format("delta").mode("append").save(path) // version 1
      spark.range(1).write.format("delta").mode("append").save(path) // version 2

      val kernelSnapshot2 = loadKernelSnapshot(path)
      val snapshot2 = new DeltaV2Snapshot(kernelSnapshot2)
      assert(snapshot2.version === 2L)
      assert(snapshot2.version === kernelSnapshot2.getVersion)
    }
  }

  /**
   * Differential check: every member the DeltaV2Snapshot sources from Kernel must match what the V1
   * `DeltaLog.snapshot` reports for the same physical table.
   */
  private def assertMatchesV1(path: String): Unit = {
    val v1 = DeltaLog.forTable(spark, path).update()
    val v2 = new DeltaV2Snapshot(loadKernelSnapshot(path))

    assert(v2.version === v1.version)
    assert(v2.metadata.id === v1.metadata.id)
    assert(v2.metadata.schemaString === v1.metadata.schemaString)
    assert(v2.metadata.partitionColumns === v1.metadata.partitionColumns)
    assert(v2.metadata.configuration === v1.metadata.configuration)
    assert(v2.protocol === v1.protocol)
    assert(v2.schema === v1.schema)
    assert(v2.columnMappingMode === v1.columnMappingMode)
    assert(v2.numOfFiles === v1.numOfFiles)
    assert(v2.dataPath === v1.dataPath)
    // With ICT disabled, both read the commit-file modification time for the same commit.
    assert(v2.timestamp === v1.timestamp)
    // A Kernel-backed snapshot has no V1 DeltaLog from which to derive usage-log tags.
    assert(v2.getCommonTags.isEmpty)

    // allFiles is read through Kernel but must describe the same physical files as V1: same paths,
    // sizes, and partition values (compared as a set -- ordering is not guaranteed).
    def fileKeys(s: Snapshot) =
      s.allFiles.collect().map(f => (f.path, f.size, f.partitionValues)).toSet
    assert(fileKeys(v2) === fileKeys(v1))

    // Both bin the same file sizes with the same default bins, so the histograms agree.
    assert(v2.sizeInBytes === v1.sizeInBytes)
    assert(v2.fileSizeHistogram === v1.fileSizeHistogram)
  }

  /**
   * A table shape to differential-test. `build` writes the table at `path` (through V1); the case
   * exercises a distinct snapshot permutation (partitioning, multiple commits, a checkpoint, or
   * deletion vectors).
   */
  private case class SnapshotCase(name: String, build: String => Unit)

  private val snapshotCases: Seq[SnapshotCase] = Seq(
    SnapshotCase("unpartitioned, single commit", path =>
      spark.range(10).write.format("delta").save(path)),
    SnapshotCase("unpartitioned, multiple commits", { path =>
      spark.range(10).write.format("delta").save(path)
      spark.range(10, 20).write.format("delta").mode("append").save(path)
      spark.range(20, 30).write.format("delta").mode("append").save(path)
    }),
    SnapshotCase("partitioned", path =>
      spark.range(20).selectExpr("id", "id % 3 as part")
        .write.format("delta").partitionBy("part").save(path)),
    SnapshotCase("partitioned, multiple commits", { path =>
      spark.range(20).selectExpr("id", "id % 3 as part")
        .write.format("delta").partitionBy("part").save(path)
      spark.range(20, 40).selectExpr("id", "id % 3 as part")
        .write.format("delta").partitionBy("part").mode("append").save(path)
    }),
    SnapshotCase("with a checkpoint", { path =>
      spark.range(10).write.format("delta").save(path)
      spark.range(10, 20).write.format("delta").mode("append").save(path)
      DeltaLog.forTable(spark, path).checkpoint()
      spark.range(20, 30).write.format("delta").mode("append").save(path)
    }),
    SnapshotCase("with deletion vectors", { path =>
      spark.range(20).write.format("delta")
        .option("delta.enableDeletionVectors", "true").save(path)
      spark.sql(s"DELETE FROM delta.`$path` WHERE id % 4 = 0")
    }),
    // With in-commit timestamps enabled, `timestamp` reads the stored ICT rather than the commit
    // file's mtime; both V1 and Kernel read the same persisted value, so they must still match.
    SnapshotCase("with in-commit timestamps", { path =>
      spark.range(10).write.format("delta")
        .option("delta.enableInCommitTimestamps", "true").save(path)
      spark.range(10, 20).write.format("delta").mode("append").save(path)
    })
  )

  snapshotCases.foreach { c =>
    test(s"data members match the V1 snapshot -- ${c.name}") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        c.build(path)
        assertMatchesV1(path)
      }
    }
  }

  test("V1 state-reconstruction members are not yet supported on a DeltaV2 snapshot") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(1).write.format("delta").save(path)

      val snapshot = new DeltaV2Snapshot(loadKernelSnapshot(path))

      // These still fail loudly so an unmigrated caller cannot silently read empty V1 state.
      intercept[UnsupportedOperationException](snapshot.stateDF)
      intercept[UnsupportedOperationException](snapshot.stateDS)
      intercept[UnsupportedOperationException](snapshot.tombstones)
      intercept[UnsupportedOperationException](snapshot.computeChecksum)
      intercept[UnsupportedOperationException](snapshot.deltaFileIndexOpt)
      intercept[UnsupportedOperationException](snapshot.checkpointProvider)

      assert(snapshot.deltaFileSizeInBytes() === -1L)
      assert(snapshot.checkpointSizeInBytes() === -1L)
    }
  }

  test("domainMetadata is served correctly") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.sql(
        s"CREATE TABLE delta.`$path` (id LONG) USING delta " +
          "TBLPROPERTIES ('delta.feature.domainMetadata' = 'supported')")

      // Use DeltaLog to create a domain metadata easier for DeltaV2Snapshot read testing.
      DeltaLog.forTable(spark, path).startTransaction().commit(
        DomainMetadata("test.userDomain", """{"key":"value"}""", removed = false) :: Nil,
        DeltaOperations.ManualUpdate)

      val snapshot = new DeltaV2Snapshot(loadKernelSnapshot(path))

      val userDomain = snapshot.domainMetadata.find(_.domain == "test.userDomain")
        .getOrElse(fail("expected the test.userDomain domain"))
      assert(!userDomain.removed)
      assert(userDomain.configuration === """{"key":"value"}""")
      // Inherited from SnapshotStateManager (not overridden here): Kernel serves domain metadata
      // without V1 state reconstruction, so it is always "known".
      assert(snapshot.domainMetadatasIfKnown.contains(snapshot.domainMetadata))
    }
  }

  test("fileSizeHistogram is served from the checksum when one exists") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 1000).write.format("delta").save(path)
      spark.range(0, 5).write.format("delta").mode("append").save(path)
      val kernelSnapshot = loadKernelSnapshot(path)
      assert(kernelSnapshot.getCurrentCrcInfo.get().getFileSizeHistogram.isPresent)
      val snapshot = new DeltaV2Snapshot(kernelSnapshot)

      assert(snapshot.fileSizeHistogram.nonEmpty)
      assert(
        snapshot.fileSizeHistogram.get.sortedBinBoundaries ===
        FileSizeHistogramUtils.DEFAULT_BINS)
      assert(
        snapshot.fileSizeHistogram.get.fileCounts.sum === snapshot.numOfFiles
      )
      assert(
        snapshot.fileSizeHistogram.get.totalBytes.sum === snapshot.sizeInBytes
      )
    }
  }

  test("fileSizeHistogram is None when the checksum has none") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      withSQLConf(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "false") {
        spark.range(0, 1000).write.format("delta").save(path)
      }
      val snapshot = new DeltaV2Snapshot(loadKernelSnapshot(path))
      assert(snapshot.fileSizeHistogram.isEmpty)
    }
  }

  test("sizeInBytes is served from the checksum when one exists") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 1000).write.format("delta").save(path)

      val kernelSnapshot = loadKernelSnapshot(path)
      assert(kernelSnapshot.getCurrentCrcInfo.isPresent)

      val snapshot = new DeltaV2Snapshot(kernelSnapshot)
      assert(snapshot.sizeInBytesIfKnown.nonEmpty)
      assert(
        snapshot.sizeInBytes === snapshot.allFiles.collect().map(_.size).sum
      )
    }
  }

  test("sizeInBytes is -1 when the checksum has none") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      withSQLConf(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "false") {
        spark.range(0, 1000).write.format("delta").save(path)
      }

      val snapshot = new DeltaV2Snapshot(loadKernelSnapshot(path))
      assert(snapshot.sizeInBytesIfKnown.isEmpty)
      assert(snapshot.sizeInBytes === -1L)
    }
  }

  test("fileSizeHistogram is None when the histogram config is disabled") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(5).write.format("delta").save(path)

      withSQLConf(DeltaSQLConf.DELTA_FILE_SIZE_HISTOGRAM_ENABLED.key -> "false") {
        assert(new DeltaV2Snapshot(loadKernelSnapshot(path)).fileSizeHistogram.isEmpty)
      }
    }
  }

  test("domainMetadata is empty for a table with no domains") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(1).write.format("delta").save(path)

      val snapshot = new DeltaV2Snapshot(loadKernelSnapshot(path))
      assert(snapshot.domainMetadata.isEmpty)
    }
  }

  test("allFiles uses the active Spark session instead of storing a construction session") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(1).write.format("delta").save(path)
      val snapshot = new DeltaV2Snapshot(loadKernelSnapshot(path))
      val activeSession = spark.newSession()

      activeSession.withActive {
        assert(snapshot.allFiles.sparkSession eq activeSession)
        assert(snapshot.allFiles.count() === 1L)
      }
    }
  }

  test("getKernelSnapshot returns the wrapped Kernel snapshot") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(1).write.format("delta").save(path)
      val kernelSnapshot = loadKernelSnapshot(path)
      val snapshot = new DeltaV2Snapshot(kernelSnapshot)

      assert(DeltaV2Snapshot.getKernelSnapshot(snapshot) eq kernelSnapshot)
    }
  }

  test("getKernelSnapshot rejects a non-Kernel-backed V1 snapshot") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(1).write.format("delta").save(path)
      val snapshot = DeltaLog.forTable(spark, path).update()

      val error = intercept[IllegalStateException] {
        DeltaV2Snapshot.getKernelSnapshot(snapshot)
      }
      assert(error.getMessage.contains("Expected a DeltaV2Snapshot"))
      assert(error.getMessage.contains(snapshot.getClass.getName))
    }
  }

  test("getKernelSnapshot rejects null") {
    val error = intercept[NullPointerException] {
      DeltaV2Snapshot.getKernelSnapshot(null)
    }
    assert(error.getMessage === "snapshot is null")
  }

}
