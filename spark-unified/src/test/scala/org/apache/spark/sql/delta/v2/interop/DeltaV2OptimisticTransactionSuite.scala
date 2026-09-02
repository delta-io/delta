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

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import org.apache.spark.sql.delta.DeltaOperations
import org.apache.spark.sql.delta.actions.{AddFile, SetTransaction}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import io.delta.spark.internal.v2.kernel.KernelEngineFactory
import io.delta.kernel.Table
import io.delta.kernel.internal.SnapshotImpl

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType
import org.apache.spark.util.SystemClock

/**
 * Tests for [[DeltaV2OptimisticTransaction]]. Two groups:
 *  - Construction: a transaction builds over a Kernel snapshot (with a null V1 `deltaLog`) and its
 *    read state (`readVersion`, `metadata`, `protocol`, `snapshot`) resolves from the wrapped
 *    [[DeltaV2Snapshot]].
 *  - Commit: [[AddFile]] actions commit through Kernel's `Transaction.commit` while the surrounding
 *    commit machinery runs unchanged; the durable log is asserted by re-loading the table through
 *    Kernel as a [[DeltaV2Snapshot]]. Non-AddFile actions fail loudly.
 */
class DeltaV2OptimisticTransactionSuite
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  import testImplicits._

  /** Builds a Kernel-backed transaction over the latest snapshot of the table at `dir`. */
  private def startKernelTxn(dir: File): DeltaV2OptimisticTransaction = {
    // scalastyle:off deltahadoopconfiguration
    // No DeltaLog here (the snapshot is loaded via Kernel), so use the session Hadoop conf.
    val engine = KernelEngineFactory.createDefaultEngine(spark.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration
    val kernelSnap = Table
      .forPath(engine, dir.getCanonicalPath)
      .getLatestSnapshot(engine)
      .asInstanceOf[SnapshotImpl]
    val deltaV2Snapshot = new DeltaV2Snapshot(kernelSnap)
    new DeltaV2OptimisticTransaction(catalogTable = None, deltaV2Snapshot, engine)
  }

  private def latestKernelSnapshot(dir: File): DeltaV2Snapshot = {
    // scalastyle:off deltahadoopconfiguration
    val engine = KernelEngineFactory.createDefaultEngine(spark.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration
    val kernelSnap = Table
      .forPath(engine, dir.getCanonicalPath)
      .getLatestSnapshot(engine)
      .asInstanceOf[SnapshotImpl]
    new DeltaV2Snapshot(kernelSnap)
  }

  /** Seeds a simple (unpartitioned) V1 Delta table at `dir`. */
  private def seedTable(dir: File): Unit = {
    spark.range(0, 5).toDF("id").coalesce(1)
      .write.format("delta").save(dir.getCanonicalPath)
  }

  test("constructor succeeds and readVersion matches the seeded version") {
    withTempDir { dir =>
      seedTable(dir) // version 0
      assert(startKernelTxn(dir).readVersion === 0L)

      spark.range(0, 1).toDF("id")
        .write.format("delta").mode("append").save(dir.getCanonicalPath) // version 1
      assert(startKernelTxn(dir).readVersion === 1L)
    }
  }

  test("metadata and protocol resolve from the Kernel snapshot") {
    withTempDir { dir =>
      seedTable(dir)
      val txn = startKernelTxn(dir)

      // Read state reflects the seeded table: a single LONG `id` column, unpartitioned.
      assert(txn.metadata.schema.fieldNames.toSeq === Seq("id"))
      assert(txn.metadata.schema("id").dataType === LongType)
      assert(txn.metadata.partitionColumns.isEmpty)
      assert(txn.metadata.id.nonEmpty)

      // A basic table resolves a protocol with at least the minimum supported versions.
      assert(txn.protocol.minReaderVersion >= 1)
      assert(txn.protocol.minWriterVersion >= 2)

      // Kernel-sourced path overrides point at the table directory and its `_delta_log` child.
      assert(txn.dataPath.getName === dir.getName)
      assert(txn.logPath.getName === "_delta_log")
      assert(txn.logPath.getParent.getName === dir.getName)
    }
  }

  test("null-deltaLog construction overrides resolve without a V1 DeltaLog") {
    withTempDir { dir =>
      seedTable(dir)
      val txn = startKernelTxn(dir)

      // newDeltaHadoopConf: sourced from the session conf (no V1 deltaLog); non-null and usable.
      val conf = txn.newDeltaHadoopConf()
      assert(conf != null)
      // Usable as a real Hadoop conf: can resolve a FileSystem for the table path.
      assert(txn.dataPath.getFileSystem(conf) != null)

      // clock: a Kernel-backed txn has no V1 DeltaLog clock; defaults to a SystemClock.
      assert(txn.clock.isInstanceOf[SystemClock])

    }
  }

  test("snapshot is the supplied DeltaV2Snapshot; construction works for a partitioned table") {
    withTempDir { dir =>
      spark.range(0, 6).toDF("id")
        .selectExpr("id", "id % 2 as p")
        .write.format("delta").partitionBy("p").save(dir.getCanonicalPath)

      val txn = startKernelTxn(dir)
      assert(txn.snapshot.isInstanceOf[DeltaV2Snapshot])
      assert(txn.metadata.partitionColumns === Seq("p"))
    }
  }

  test("blind append of a synthetic AddFile commits through Kernel") {
    withTempDir { dir =>
      seedTable(dir)
      val txn = startKernelTxn(dir)
      assert(txn.readVersion === 0L)

      val add = AddFile(
        path = "synthetic-file",
        partitionValues = Map.empty,
        size = 1L,
        modificationTime = 1L,
        dataChange = true)
      txn.commit(add :: Nil, DeltaOperations.ManualUpdate)

      val post = latestKernelSnapshot(dir)
      assert(post.version === 1L)
      assert(post.allFiles.collect().map(_.path).contains("synthetic-file"))
    }
  }

  test("appended parquet file is readable end to end") {
    withTempDir { dir =>
      seedTable(dir)

      // Stage a real parquet file with the table's schema in a sibling directory (writing
      // format("parquet") under the table root trips DELTA_INVALID_FORMAT validation), then copy
      // it into the table dir, mirroring how OptimisticTransactionSuite drives commits at the
      // file-action level.
      val staging = new File(dir.getParentFile, s"${dir.getName}-staging")
      spark.range(5, 7).toDF("id").coalesce(1)
        .write.parquet(staging.getCanonicalPath)
      val parquetFile = staging.listFiles()
        .filter(f => f.getName.endsWith(".parquet") && !f.getName.startsWith("_"))
        .head
      val targetName = s"part-kernel-poc-${parquetFile.getName}"
      val target = new File(dir, targetName)
      Files.copy(parquetFile.toPath, target.toPath, StandardCopyOption.REPLACE_EXISTING)

      val txn = startKernelTxn(dir)
      val add = AddFile(
        path = targetName,
        partitionValues = Map.empty,
        size = target.length(),
        modificationTime = target.lastModified(),
        dataChange = true)
      txn.commit(add :: Nil, DeltaOperations.ManualUpdate)

      checkAnswer(
        spark.read.format("delta").load(dir.getCanonicalPath),
        Seq(0L, 1L, 2L, 3L, 4L, 5L, 6L).toDF("id"))
    }
  }

  test("consecutive kernel commits advance the version") {
    withTempDir { dir =>
      seedTable(dir)
      (1 to 3).foreach { i =>
        val txn = startKernelTxn(dir)
        assert(txn.readVersion === (i - 1).toLong)
        val add = AddFile(s"synthetic-$i", Map.empty, 1L, 1L, dataChange = true)
        txn.commit(add :: Nil, DeltaOperations.ManualUpdate)
      }
      val post = latestKernelSnapshot(dir)
      assert(post.version === 3L)
      assert(post.allFiles.collect().map(_.path).count(_.startsWith("synthetic-")) === 3)
    }
  }

  test("unsupported actions fail loudly") {
    withTempDir { dir =>
      seedTable(dir)
      val txn = startKernelTxn(dir)
      val setTxn = SetTransaction("test-app", 1L, Some(1L))
      val e = intercept[UnsupportedOperationException] {
        txn.commit(setTxn :: Nil, DeltaOperations.ManualUpdate)
      }
      assert(e.getMessage.contains("kernel wrapper gap"))
    }
  }

  /**
   * Commits a synthetic [[AddFile]] carrying `statsJson` through Kernel. Used by the stats tests
   * below to drive `kernelStatistics` with a chosen stats payload.
   */
  private def commitWithStats(dir: File, statsJson: String): Unit = {
    val txn = startKernelTxn(dir)
    val add = AddFile(
      path = "synthetic-stats-file",
      partitionValues = Map.empty,
      size = 1L,
      modificationTime = 1L,
      dataChange = true,
      stats = statsJson)
    txn.commit(add :: Nil, DeltaOperations.ManualUpdate)
  }

  test("full stats JSON round-trips into the kernel add action") {
    withTempDir { dir =>
      seedTable(dir)
      commitWithStats(
        dir,
        """{"numRecords":3,"minValues":{"id":1},"maxValues":{"id":9},"nullCount":{"id":0}}""")

      val post = latestKernelSnapshot(dir)
      assert(post.version === 1L)
      val committed = post.allFiles.collect().find(_.path == "synthetic-stats-file").get
      // Only numRecords is asserted: the native (JNR) FFI add-file path (ActionRowConverter)
      // intentionally carries numRecords only, so min/max/nullCount are not populated.
      assert(committed.stats.contains("\"numRecords\":3"))
    }
  }

  test("stats-less AddFile commits with empty stats rather than failing") {
    withTempDir { dir =>
      seedTable(dir)
      // stats defaults to null on AddFile; absent stats are legitimate and must not error.
      commitWithStats(dir, statsJson = null)
      assert(latestKernelSnapshot(dir).version === 1L)
    }
  }

  test("numRecords-only stats JSON commits (no min/max to parse)") {
    withTempDir { dir =>
      seedTable(dir)
      commitWithStats(dir, """{"numRecords":7}""")

      val post = latestKernelSnapshot(dir)
      val committed = post.allFiles.collect().find(_.path == "synthetic-stats-file").get
      assert(committed.stats.contains("\"numRecords\":7"))
    }
  }

  test("malformed stats JSON fails the commit instead of silently dropping stats") {
    withTempDir { dir =>
      seedTable(dir)
      // Truncated JSON: the previous implementation swallowed this and committed
      // numRecords-only (here, no stats at all), losing data-skipping stats silently.
      val e = intercept[Exception] {
        commitWithStats(dir, """{"numRecords":3,"minValues":{"id":""")
      }
      assert(
        e.getMessage != null && e.getMessage.contains("Failed to parse JSON"),
        s"expected a JSON parse failure, got: ${e.getMessage}")
      // The bad write did not land.
      assert(latestKernelSnapshot(dir).version === 0L)
    }
  }

  test("stats JSON whose value type contradicts the schema fails the commit") {
    withTempDir { dir =>
      seedTable(dir)
      // `id` is a LONG; a string min value is a real stats/schema inconsistency.
      val e = intercept[Exception] {
        commitWithStats(
          dir,
          """{"numRecords":3,"minValues":{"id":"not-a-number"},"maxValues":{"id":9}}""")
      }
      assert(e.getMessage != null, "expected a typed-parse failure with a message")
      assert(latestKernelSnapshot(dir).version === 0L)
    }
  }

  /**
   * Seeds a table partitioned by a single column `p` of `partitionType`, inserts one row with
   * `partitionValueSql`, then commits a synthetic [[AddFile]] through Kernel reusing the seeded
   * row's serialized partition values. Asserts the commit succeeds and advances the version.
   *
   * This exercises the partition-value typing in `generateKernelAppendActionRows`: Kernel's
   * `getWriteContext` validates each literal's type against the partition schema with exact
   * type-equality, so a non-string partition column would fail if values were still typed as
   * strings.
   */
  private def checkTypedPartitionAppend(
      partitionType: String, partitionValueSql: String): Unit = {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.sql(
        s"""CREATE TABLE delta.`$path` (id LONG, p $partitionType)
           |USING delta PARTITIONED BY (p)""".stripMargin)
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, $partitionValueSql)")

      val base = latestKernelSnapshot(dir)
      val baseVersion = base.version
      val sample = base.allFiles.collect().head

      val txn = startKernelTxn(dir)
      val add = AddFile(
        path = s"synthetic-${sample.path}",
        partitionValues = sample.partitionValues,
        size = 1L,
        modificationTime = 1L,
        dataChange = true)
      txn.commit(add :: Nil, DeltaOperations.ManualUpdate)

      val post = latestKernelSnapshot(dir)
      assert(post.version === baseVersion + 1)
      assert(post.allFiles.collect().map(_.path).exists(_.startsWith("synthetic-")))
    }
  }

  test("append to int-partitioned table types the partition literal") {
    checkTypedPartitionAppend("INT", "5")
  }

  test("append to date-partitioned table types the partition literal") {
    checkTypedPartitionAppend("DATE", "DATE'2024-03-11'")
  }

  test("append to timestamp-partitioned table types the partition literal") {
    checkTypedPartitionAppend("TIMESTAMP", "TIMESTAMP'2024-03-11 11:00:00.123456'")
  }

  test("append to decimal-partitioned table types the partition literal") {
    checkTypedPartitionAppend("DECIMAL(10,2)", "CAST(12.34 AS DECIMAL(10,2))")
  }

  test("append to boolean-partitioned table types the partition literal") {
    checkTypedPartitionAppend("BOOLEAN", "true")
  }

  test("append with a null partition value types the null literal") {
    checkTypedPartitionAppend("INT", "NULL")
  }

  test("append to string-partitioned table still works") {
    checkTypedPartitionAppend("STRING", "'foo'")
  }

  test("multiple AddFiles per partition all commit through Kernel") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.sql(
        s"""CREATE TABLE delta.`$path` (id LONG, p INT)
           |USING delta PARTITIONED BY (p)""".stripMargin)
      // Two partitions (p=0, p=1), each with a real data file so we can reuse their serialized
      // partition values below.
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 0), (2, 1)")

      val base = latestKernelSnapshot(dir)
      val baseVersion = base.version
      // partitionValues keyed by partition -> one representative file per partition.
      val samplesByPartition =
        base.allFiles.collect().groupBy(_.partitionValues).map {
          case (partitionValues, files) => partitionValues -> files.head
        }
      assert(samplesByPartition.size === 2, "expected one file per partition to reuse")

      // Stage two synthetic AddFiles per partition (four total) so the per-partition grouping in
      // generateKernelAppendActionRows must emit multiple append action rows for the same write
      // context, not just one.
      val adds = samplesByPartition.toSeq.flatMap { case (partitionValues, sample) =>
        (1 to 2).map { i =>
          AddFile(
            path = s"synthetic-p${partitionValues.values.head}-$i-${sample.path}",
            partitionValues = partitionValues,
            size = 1L,
            modificationTime = 1L,
            dataChange = true)
        }
      }
      assert(adds.size === 4)

      val txn = startKernelTxn(dir)
      txn.commit(adds.toList, DeltaOperations.ManualUpdate)

      val post = latestKernelSnapshot(dir)
      assert(post.version === baseVersion + 1)
      val committedPaths = post.allFiles.collect().map(_.path)
      // All four synthetic files landed, spread across both partitions.
      adds.foreach(add => assert(committedPaths.contains(add.path), s"missing ${add.path}"))
      assert(committedPaths.count(_.startsWith("synthetic-p0")) === 2)
      assert(committedPaths.count(_.startsWith("synthetic-p1")) === 2)
    }
  }
}
