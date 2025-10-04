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

import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.exceptions.TableNotFoundException
import io.delta.kernel.internal.DeltaLogActionUtils
import io.delta.kernel.internal.DeltaLogActionUtils.listDeltaLogFilesAsIter
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames

import org.scalatest.funsuite.AnyFunSuite

/** Test suite for end-to-end cases. See also the mocked unit tests in DeltaLogActionUtilsSuite. */
class DeltaLogActionUtilsE2ESuite extends AnyFunSuite with TestUtils {
  test("listDeltaLogFiles: throws TableNotFoundException if _delta_log does not exist") {
    withTempDir { tableDir =>
      intercept[TableNotFoundException] {
        listDeltaLogFilesAsIter(
          defaultEngine,
          Set(FileNames.DeltaLogFileType.COMMIT, FileNames.DeltaLogFileType.CHECKPOINT).asJava,
          new Path(tableDir.getAbsolutePath),
          0,
          Optional.empty(),
          true /* mustBeRecreatable */
        ).toInMemoryList
      }
    }
  }

  test("listDeltaLogFiles: returns empty list if _delta_log is empty") {
    withTempDir { tableDir =>
      val logDir = new File(tableDir, "_delta_log")
      assert(logDir.mkdirs() && logDir.isDirectory && logDir.listFiles().isEmpty)

      val result = listDeltaLogFilesAsIter(
        defaultEngine,
        Set(FileNames.DeltaLogFileType.COMMIT, FileNames.DeltaLogFileType.CHECKPOINT).asJava,
        new Path(tableDir.getAbsolutePath),
        0,
        Optional.empty(),
        true /* mustBeRecreatable */
      ).toInMemoryList

      assert(result.isEmpty)
    }
  }

  test("getCommitsFromCommitFilesWithProtocolValidation: returns iterator of CommitActions") {
    withTempDir { tableDir =>
      // Create a Delta table using Spark
      val tablePath = tableDir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$tablePath` (id INT, value STRING) USING delta")
      spark.sql(s"INSERT INTO delta.`$tablePath` VALUES (1, 'v1'), (2, 'v2')")
      spark.sql(s"INSERT INTO delta.`$tablePath` VALUES (3, 'v3')")
      spark.sql(s"INSERT INTO delta.`$tablePath` VALUES (4, 'v4')")

      // Get commit files for version 1-3 using listDeltaLogFilesAsIter
      val commitFiles = listDeltaLogFilesAsIter(
        defaultEngine,
        java.util.Collections.singleton(FileNames.DeltaLogFileType.COMMIT),
        new Path(tablePath),
        1,
        Optional.of(3L),
        false /* mustBeRecreatable */
      ).toInMemoryList

      // Call getCommitsFromCommitFilesWithProtocolValidation
      val actionSet = new java.util.HashSet[DeltaLogActionUtils.DeltaAction]()
      actionSet.add(DeltaLogActionUtils.DeltaAction.ADD)
      actionSet.add(DeltaLogActionUtils.DeltaAction.REMOVE)

      val commitsIter = DeltaLogActionUtils.getCommitsFromCommitFilesWithProtocolValidation(
        defaultEngine,
        tablePath,
        commitFiles,
        actionSet)

      try {
        // Verify we get 3 commits (versions 1, 2, 3)
        val commits = commitsIter.toSeq
        assert(commits.size == 3, s"Expected 3 commits, got ${commits.size}")

        // Verify version and timestamp for each commit
        commits.zipWithIndex.foreach { case (commitActions, idx) =>
          val expectedVersion = idx + 1
          assert(
            commitActions.getVersion == expectedVersion,
            s"Expected version $expectedVersion, got ${commitActions.getVersion}")
          assert(
            commitActions.getTimestamp > 0,
            s"Expected positive timestamp, got ${commitActions.getTimestamp}")

          // Verify we can iterate over actions
          val actionsIter = commitActions.getActions
          try {
            var batchCount = 0
            var totalRowCount = 0
            while (actionsIter.hasNext) {
              val batch = actionsIter.next()
              batchCount += 1
              totalRowCount += batch.getSize

              // Verify schema contains add/remove columns
              val schema = batch.getSchema
              assert(
                schema.indexOf("add") >= 0 || schema.indexOf("remove") >= 0,
                s"Expected 'add' or 'remove' column in schema")
            }
            // Each insert should produce at least 1 batch with actions
            assert(batchCount > 0, s"Expected at least 1 batch for version $expectedVersion")
            assert(
              totalRowCount > 0,
              s"Expected at least 1 action for version $expectedVersion")
          } finally {
            actionsIter.close()
          }

          commitActions.close()
        }
      } finally {
        commitsIter.close()
      }
    }
  }

  test("getCommitsFromCommitFilesWithProtocolValidation: extracts timestamp from commitInfo") {
    withTempDir { tableDir =>
      val tablePath = tableDir.getAbsolutePath

      // Create table and insert data
      spark.sql(s"CREATE TABLE delta.`$tablePath` (id INT, value STRING) USING delta")
      spark.sql(s"INSERT INTO delta.`$tablePath` VALUES (1, 'v1')")

      // Get version 1 commit file
      val commitFiles = listDeltaLogFilesAsIter(
        defaultEngine,
        java.util.Collections.singleton(FileNames.DeltaLogFileType.COMMIT),
        new Path(tablePath),
        1,
        Optional.of(1L),
        false /* mustBeRecreatable */
      ).toInMemoryList

      val actionSet = new java.util.HashSet[DeltaLogActionUtils.DeltaAction]()
      actionSet.add(DeltaLogActionUtils.DeltaAction.ADD)

      val commitsIter = DeltaLogActionUtils.getCommitsFromCommitFilesWithProtocolValidation(
        defaultEngine,
        tablePath,
        commitFiles,
        actionSet)

      try {
        val commits = commitsIter.toSeq
        assert(commits.size == 1)

        val commitActions = commits.head
        assert(commitActions.getVersion == 1)

        // Verify timestamp is extracted (either from commitInfo.inCommitTimestamp or file modtime)
        val timestamp = commitActions.getTimestamp
        assert(timestamp > 0, s"Expected positive timestamp, got $timestamp")

        commitActions.close()
      } finally {
        commitsIter.close()
      }
    }
  }

  test(
    "getCommitsFromCommitFilesWithProtocolValidation: lazy loading (actions not read if getActions not called)") {
    withTempDir { tableDir =>
      val tablePath = tableDir.getAbsolutePath

      // Create table with 3 inserts
      spark.sql(s"CREATE TABLE delta.`$tablePath` (id INT, value STRING) USING delta")
      spark.sql(s"INSERT INTO delta.`$tablePath` VALUES (1, 'v1')")
      spark.sql(s"INSERT INTO delta.`$tablePath` VALUES (2, 'v2')")
      spark.sql(s"INSERT INTO delta.`$tablePath` VALUES (3, 'v3')")

      // Get commit files for all 3 versions
      val commitFiles = listDeltaLogFilesAsIter(
        defaultEngine,
        java.util.Collections.singleton(FileNames.DeltaLogFileType.COMMIT),
        new Path(tablePath),
        1,
        Optional.of(3L),
        false /* mustBeRecreatable */
      ).toInMemoryList

      val actionSet = new java.util.HashSet[DeltaLogActionUtils.DeltaAction]()
      actionSet.add(DeltaLogActionUtils.DeltaAction.ADD)

      val commitsIter = DeltaLogActionUtils.getCommitsFromCommitFilesWithProtocolValidation(
        defaultEngine,
        tablePath,
        commitFiles,
        actionSet)

      try {
        var commitCount = 0
        while (commitsIter.hasNext) {
          val commit = commitsIter.next()
          commitCount += 1

          // Access metadata (should not trigger file read)
          val version = commit.getVersion
          val timestamp = commit.getTimestamp
          assert(version >= 1 && version <= 3)
          assert(timestamp > 0)

          // Only read actions for version 2 (simulating skip of other versions)
          if (version == 2) {
            val actionsIter = commit.getActions
            try {
              var actionCount = 0
              while (actionsIter.hasNext) {
                val batch = actionsIter.next()
                actionCount += batch.getSize
              }
              assert(actionCount > 0, s"Expected actions for version $version")
            } finally {
              actionsIter.close()
            }
          }
          // For versions 1 and 3, we never call getActions()
          // This simulates skipping commits without reading their files

          commit.close()
        }

        assert(commitCount == 3, s"Expected 3 commits, got $commitCount")

        // If lazy loading works correctly, only version 2's file was read
        // (We can't easily verify this without instrumentation, but the test
        // demonstrates the API usage pattern for skipping commits)
      } finally {
        commitsIter.close()
      }
    }
  }
}
