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

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import io.delta.kernel.Table
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.internal.SnapshotImpl

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.SystemClock

/**
 * Tests for the [[DeltaV2OptimisticTransaction]] skeleton. This suite exercises the construction
 * path only: a transaction can be built over a Kernel snapshot (with a null V1 `deltaLog`) and its
 * read state (`readVersion`, `metadata`, `protocol`, `snapshot`) resolves from the wrapped
 * [[DeltaV2Snapshot]]. The commit lifecycle is not yet implemented and must fail loudly.
 */
class DeltaV2OptimisticTransactionSuite
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  /** Builds a Kernel-backed transaction over the latest snapshot of the table at `dir`. */
  private def startKernelTxn(dir: File): DeltaV2OptimisticTransaction = {
    // scalastyle:off deltahadoopconfiguration
    // No DeltaLog here (the snapshot is loaded via Kernel), so use the session Hadoop conf.
    val engine = DefaultEngine.create(spark.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration
    val kernelSnap = Table
      .forPath(engine, dir.getCanonicalPath)
      .getLatestSnapshot(engine)
      .asInstanceOf[SnapshotImpl]
    val deltaV2Snapshot = new DeltaV2Snapshot(kernelSnap, spark)
    new DeltaV2OptimisticTransaction(catalogTable = None, deltaV2Snapshot)
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

  test("metadata and protocol resolve from the Kernel snapshot and match V1") {
    withTempDir { dir =>
      seedTable(dir)
      val txn = startKernelTxn(dir)
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val v1 = log.update()

      assert(txn.metadata.id === v1.metadata.id)
      assert(txn.metadata.schemaString === v1.metadata.schemaString)
      assert(txn.metadata.partitionColumns === v1.metadata.partitionColumns)
      assert(txn.protocol === v1.protocol)

      // The Kernel-sourced path overrides match the V1 DeltaLog's paths.
      assert(txn.dataPath === log.dataPath)
      assert(txn.logPath === log.logPath)
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
      assert(txn.snapshot eq txn.deltaV2Snapshot)
      assert(txn.deltaV2Snapshot.isInstanceOf[DeltaV2Snapshot])
      assert(txn.metadata.partitionColumns === Seq("p"))
    }
  }

  test("commit path is not implemented and fails loudly") {
    withTempDir { dir =>
      seedTable(dir)
      val txn = startKernelTxn(dir)
      // Call the commit chokepoint directly: going through txn.commit(...) would first reach the
      // V1 prepareCommit machinery, which NPEs on the null deltaLog before this seam.
      val e = intercept[UnsupportedOperationException] {
        txn.writeCommitFile(
          attemptVersion = txn.readVersion + 1L,
          jsonActions = Iterator.empty,
          currentTransactionInfo = null)
      }
      assert(e.getMessage.contains("not implemented"))
    }
  }
}
