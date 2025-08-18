/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.kernel.defaults

import java.io.File
import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.{Operation, TableManager}
import io.delta.kernel.defaults.utils.WriteUtils
import io.delta.kernel.internal.actions.Protocol
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.internal.util.DirectoryCreationUtils.createAllDeltaDirectoriesAsNeeded
import io.delta.kernel.test.ActionUtils
import io.delta.kernel.utils.CloseableIterable.emptyIterable

import org.scalatest.funsuite.AnyFunSuite

class DirectoryCreationSuite extends AnyFunSuite with WriteUtils with ActionUtils {

  ///////////////
  // E2E Tests //
  ///////////////

  test("create table with catalogManaged & v2Checkpoints supported: _delta_log, _staged_commits, " +
    "_sidecars directories are created") {
    withTempDirAndEngine { (tablePath, engine) =>
      val logDir = new File(tablePath, "_delta_log")
      val stagedCommitsDir = new File(logDir.toString, "_staged_commits")
      val sidecarDir = new File(logDir.toString, "_sidecars")

      TableManager
        .buildCreateTableTransaction(tablePath, testSchema, "engineInfo")
        .withTableProperties(Map(
          "delta.feature.catalogOwned-preview" -> "supported",
          "delta.checkpointPolicy" -> "v2").asJava)
        .withCommitter(committerUsingPutIfAbsent)
        .build(engine)
        .commit(engine, emptyIterable())

      assert(logDir.exists() && logDir.isDirectory())
      assert(stagedCommitsDir.exists() && stagedCommitsDir.isDirectory())
      assert(sidecarDir.exists() && sidecarDir.isDirectory())
    }
  }

  test("enable catalogManaged & v2Checkpoints on existing table: _staged_commits and _sidecars " +
    "directories are created") {
    withTempDirAndEngine { (tablePath, engine) =>
      val logDir = new File(tablePath, "_delta_log")
      val stagedCommitsDir = new File(logDir.toString, "_staged_commits")
      val sidecarDir = new File(logDir.toString, "_sidecars")

      getCreateTxn(engine, tablePath, testSchema).commit(engine, emptyIterable())

      assert(logDir.exists() && logDir.isDirectory())
      assert(!sidecarDir.exists())
      assert(!stagedCommitsDir.exists())

      TableManager
        .loadSnapshot(tablePath)
        .withCommitter(committerUsingPutIfAbsent)
        .build(engine)
        .buildUpdateTableTransaction("engineInfo", Operation.MANUAL_UPDATE)
        .withTablePropertiesAdded(Map(
          "delta.feature.catalogOwned-preview" -> "supported",
          "delta.checkpointPolicy" -> "v2").asJava)
        .build(engine)
        .commit(engine, emptyIterable())

      assert(stagedCommitsDir.exists() && stagedCommitsDir.isDirectory())
      assert(sidecarDir.exists() && sidecarDir.isDirectory())
    }
  }

  ////////////////
  // Unit tests //
  ////////////////

  val basicProtocol = new Protocol(1, 2)

  val protocolWithCatalogManagedAndV2CheckpointSupport =
    new Protocol(
      TableFeatures.TABLE_FEATURES_MIN_READER_VERSION,
      TableFeatures.TABLE_FEATURES_MIN_WRITER_VERSION,
      Set(
        TableFeatures.CATALOG_MANAGED_R_W_FEATURE_PREVIEW.featureName(),
        TableFeatures.CHECKPOINT_V2_RW_FEATURE.featureName()).asJava,
      Set(
        TableFeatures.CATALOG_MANAGED_R_W_FEATURE_PREVIEW.featureName(),
        TableFeatures.CHECKPOINT_V2_RW_FEATURE.featureName(),
        TableFeatures.IN_COMMIT_TIMESTAMP_W_FEATURE.featureName()).asJava)

  test("_delta_log -- Case: CREATE") {
    withTempDirAndEngine { (tempDir, engine) =>
      val logPath = new Path(tempDir, "_delta_log")

      createAllDeltaDirectoriesAsNeeded(
        engine,
        logPath,
        0,
        Optional.empty[Protocol](),
        basicProtocol)

      val logDir = new File(logPath.toString)
      assert(logDir.exists() && logDir.isDirectory())
    }
  }

  test("_sidecars && _staged_commits -- Case: CREATE") {
    withTempDirAndEngine { (tempDir, engine) =>
      val logPath = new Path(tempDir, "_delta_log")

      createAllDeltaDirectoriesAsNeeded(
        engine,
        logPath,
        0,
        Optional.empty[Protocol](),
        protocolWithCatalogManagedAndV2CheckpointSupport)

      // Check delta log directory
      val logDir = new File(logPath.toString)
      assert(logDir.exists() && logDir.isDirectory())

      // Check staged commits directory
      val stagedCommitDir = new File(logPath.toString, "_staged_commits")
      assert(stagedCommitDir.exists() && stagedCommitDir.isDirectory())

      // Check sidecar directory
      val sidecarDir = new File(logPath.toString, "_sidecars")
      assert(sidecarDir.exists() && sidecarDir.isDirectory())
    }
  }

  test("_sidecars && _staged_commits -- Case: UPGRADE") {
    withTempDirAndEngine { (tempDir, engine) =>
      val logPath = new Path(tempDir, "_delta_log")

      createAllDeltaDirectoriesAsNeeded(
        engine,
        logPath,
        0,
        Optional.of(basicProtocol),
        protocolWithCatalogManagedAndV2CheckpointSupport)

      // All directories should be created
      assert(new File(logPath.toString).exists())
      assert(new File(logPath.toString, "_staged_commits").exists())
      assert(new File(logPath.toString, "_sidecars").exists())
    }
  }

  test("handles existing directories gracefully") {
    withTempDirAndEngine { (tempDir, engine) =>
      val logPath = new Path(tempDir, "_delta_log")

      // Pre-create all directories
      new File(logPath.toString).mkdirs()
      new File(logPath.toString, "_staged_commits").mkdirs()
      new File(logPath.toString, "_sidecars").mkdirs()

      // Should not throw exception when directories already exist
      createAllDeltaDirectoriesAsNeeded(
        engine,
        logPath,
        0,
        Optional.empty[Protocol](),
        protocolWithCatalogManagedAndV2CheckpointSupport)

      // Directories should still exist
      assert(new File(logPath.toString).exists())
      assert(new File(logPath.toString, "_staged_commits").exists())
      assert(new File(logPath.toString, "_sidecars").exists())
    }
  }
}
