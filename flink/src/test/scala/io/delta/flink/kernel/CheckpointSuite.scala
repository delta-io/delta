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

package io.delta.flink.kernel;

import java.util.Optional

import scala.jdk.CollectionConverters.CollectionHasAsScala

import io.delta.flink.TestHelper
import io.delta.kernel.TableManager
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.actions.AddFile
import io.delta.kernel.internal.checkpoints.CheckpointMetaData
import io.delta.kernel.internal.util.Utils.singletonCloseableIterator
import io.delta.kernel.types.{IntegerType, StructType}
import io.delta.kernel.utils.FileStatus

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class CheckpointSuite extends AnyFunSuite with TestHelper {

  test("create incremental checkpoint") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val engine = DefaultEngine.create(new Configuration)
      val schema = new StructType().add("id", IntegerType.INTEGER)

      createNonEmptyTable(
        engine,
        tablePath,
        schema,
        Seq.empty,
        properties = Map("delta.feature.v2Checkpoint" -> "supported"))

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
      val actions = files.asScala.flatMap(_.getRows.toInMemoryList.asScala).map(row =>
        new AddFile(row.getStruct(0)))
      assert(actions.size == 26)
    }
  }

  test("ignore not my checkpoints") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val engine = DefaultEngine.create(new Configuration)
      val schema = new StructType().add("id", IntegerType.INTEGER)

      createNonEmptyTable(
        engine,
        tablePath,
        schema,
        Seq.empty,
        properties = Map("delta.feature.v2Checkpoint" -> "supported"))

      for (i <- 0 until 25) {
        val snapshot = writeTable(engine, tablePath, schema, Seq.empty)
        if (i % 7 == 6) {
          snapshot.ifPresent { s =>
            s.writeCheckpoint(engine);
          }
        }
        if (i == 24) {
          new Checkpoint(engine, snapshot.get()).write();
        }
      }

      val snapshot = TableManager.loadSnapshot(tablePath).build(engine).asInstanceOf[SnapshotImpl]
      assert(snapshot.getSnapshotReport.getCheckpointVersion.get() == 25)
      val files = snapshot.getScanBuilder.build().getScanFiles(engine).toInMemoryList
      assert(files.size() == 2) // 1 checkpoint, 1 sidecars
      val actions = files.asScala.flatMap(_.getRows.toInMemoryList.asScala).map(row =>
        new AddFile(row.getStruct(0)))
      assert(actions.size == 26)
    }
  }

  test("create on older snapshots") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val engine = DefaultEngine.create(new Configuration)
      val schema = new StructType().add("id", IntegerType.INTEGER)

      createNonEmptyTable(
        engine,
        tablePath,
        schema,
        Seq.empty,
        properties = Map("delta.feature.v2Checkpoint" -> "supported"))

      for (i <- 0 until 25) {
        val snapshot = writeTable(engine, tablePath, schema, Seq.empty)
      }

      val snapshot = TableManager.loadSnapshot(tablePath).build(engine).asInstanceOf[SnapshotImpl]
      snapshot.writeCheckpoint(engine)

      val oldSnapshot = TableManager.loadSnapshot(tablePath).atVersion(20)
        .build(engine).asInstanceOf[SnapshotImpl]
      new Checkpoint(engine, oldSnapshot).write()

      val oldSnapshotAgain = TableManager.loadSnapshot(tablePath).atVersion(20)
        .build(engine).asInstanceOf[SnapshotImpl]

      assert(oldSnapshotAgain.getSnapshotReport.getCheckpointVersion.get() == 20)
      val files = oldSnapshotAgain.getScanBuilder.build().getScanFiles(engine).toInMemoryList
      assert(files.size() == 2) // 1 checkpoint, 1 sidecars
      val actions = files.asScala.flatMap(_.getRows.toInMemoryList.asScala).map(row =>
        new AddFile(row.getStruct(0)))
      assert(actions.size == 21)

      // Make sure _last_checkpoint is not changed
      val fileName = s"$tablePath/_delta_log/_last_checkpoint"
      val content = engine.getJsonHandler.readJsonFiles(
        singletonCloseableIterator(FileStatus.of(fileName)),
        CheckpointMetaData.READ_SCHEMA,
        Optional.empty).toInMemoryList
        .asScala.flatMap(_.getRows.toInMemoryList.asScala)
        .map(CheckpointMetaData.fromRow).toList
      assert(25 == content.head.version)
    }
  }
}
