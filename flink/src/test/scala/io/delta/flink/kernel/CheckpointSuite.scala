/*
 *  Copyright (2021) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.kernel;

import scala.jdk.CollectionConverters.IteratorHasAsScala

import io.delta.flink.TestHelper
import io.delta.kernel.TableManager
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.types.{IntegerType, StructType}

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class CheckpointSuite extends AnyFunSuite with TestHelper {

  test("create checkpoint") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val engine = DefaultEngine.create(new Configuration)
      val schema = new StructType().add("id", IntegerType.INTEGER)

      createNonEmptyTable(engine, tablePath, schema, Seq.empty)

      for (i <- 0 until 25) {
        val snapshot = writeTable(engine, tablePath, schema, Seq.empty)
        if (i % 7 == 6) {
          snapshot.ifPresent { s =>
            new Checkpoint(engine, s).write()
          }
        }
      }
      val snapshot = TableManager.loadSnapshot(tablePath).build(engine).asInstanceOf[SnapshotImpl]
      assert(snapshot.getSnapshotReport.getCheckpointVersion.get() == 21)
      val files = snapshot.getScanBuilder.build().getScanFiles(engine).toInMemoryList
      assert(files.size() == 8) // 4 jsons, 1 checkpoint, 3 sidecars
    }
  }
}
