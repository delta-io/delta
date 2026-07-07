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

import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.hadoop.fs.Path
import io.delta.kernel.TableManager
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.{SnapshotImpl => KernelSnapshot}

/**
 * Tests for [[DeltaV2Snapshot]] focusing on the construction surface that is wired to the
 * underlying Kernel snapshot: `path` (from `kernelSnapshot.getDataPath`) and `version` (from
 * `kernelSnapshot.getVersion`). All other members are intentionally unimplemented and are expected
 * to throw [[UnsupportedOperationException]].
 */
class DeltaV2SnapshotSuite extends DeltaSQLCommandTest {

  // scalastyle:off deltahadoopconfiguration
  // No DeltaLog in this test (the snapshot is loaded via Kernel), so use the session Hadoop conf.
  private def engine: Engine = DefaultEngine.create(spark.sessionState.newHadoopConf())
  // scalastyle:on deltahadoopconfiguration

  /** Load the latest snapshot of the Delta table at `path` via Kernel. */
  private def loadKernelSnapshot(path: String): KernelSnapshot =
    TableManager.loadSnapshot(path).build(engine).asInstanceOf[KernelSnapshot]

  test("path and version are derived from the wrapped Kernel snapshot") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(5).write.format("delta").save(path) // version 0

      val kernelSnapshot0 = loadKernelSnapshot(path)
      val snapshot0 = new DeltaV2Snapshot(kernelSnapshot0, spark)
      assert(snapshot0.version === kernelSnapshot0.getVersion)
      assert(snapshot0.version === 0L)
      assert(snapshot0.path === new Path(kernelSnapshot0.getDataPath.toString))

      // version tracks the latest committed version after more commits.
      spark.range(1).write.format("delta").mode("append").save(path) // version 1
      spark.range(1).write.format("delta").mode("append").save(path) // version 2

      val kernelSnapshot2 = loadKernelSnapshot(path)
      val snapshot2 = new DeltaV2Snapshot(kernelSnapshot2, spark)
      assert(snapshot2.version === 2L)
      assert(snapshot2.version === kernelSnapshot2.getVersion)
    }
  }

  test("unimplemented members throw UnsupportedOperationException") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(1).write.format("delta").save(path)

      val snapshot = new DeltaV2Snapshot(loadKernelSnapshot(path), spark)

      // Every externally reachable data/state member must fail loudly so that an unmigrated
      // caller cannot silently read empty V1 state.
      intercept[UnsupportedOperationException](snapshot.metadata)
      intercept[UnsupportedOperationException](snapshot.protocol)
      intercept[UnsupportedOperationException](snapshot.columnMappingMode)
      intercept[UnsupportedOperationException](snapshot.numOfFiles)
      intercept[UnsupportedOperationException](snapshot.numOfFilesIfKnown)
      intercept[UnsupportedOperationException](snapshot.sizeInBytesIfKnown)
      intercept[UnsupportedOperationException](snapshot.deltaFileIndexOpt)
      intercept[UnsupportedOperationException](snapshot.checkpointProvider)
      intercept[UnsupportedOperationException](snapshot.allFiles)
      intercept[UnsupportedOperationException](snapshot.statsColumnSpec)
      intercept[UnsupportedOperationException](snapshot.stateDF)
      intercept[UnsupportedOperationException](snapshot.stateDS)
      intercept[UnsupportedOperationException](snapshot.tombstones)
      intercept[UnsupportedOperationException](snapshot.computeChecksum)
    }
  }

}
