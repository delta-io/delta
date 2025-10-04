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

import io.delta.kernel.commit.CatalogCommitterUtils
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.internal.files.ParsedDeltaData
import io.delta.kernel.internal.util.FileNames

import org.scalatest.funsuite.AnyFunSuite

class CatalogCommitterUtilsE2ESuite extends AnyFunSuite with TestUtils {

  private def writeFile(path: String, content: String): Unit = {
    val writer = new java.io.PrintWriter(path)
    // scalastyle:off println
    try writer.println(content)
    finally writer.close()
    // scalastyle:on println
  }

  test("publishCatalogCommit: successfully publishes staged commit to delta log") {
    withTempDirAndAllDeltaSubDirs { tempDir =>
      // ===== GIVEN =====
      val tablePath = tempDir.getAbsolutePath
      val logPath = s"$tablePath/_delta_log"

      val stagedPath = FileNames.stagedCommitFile(logPath, 5L)
      writeFile(stagedPath, "foo")

      val stagedFileStatus = defaultEngine.getFileSystemClient.getFileStatus(stagedPath)
      val catalogCommit = ParsedDeltaData.forFileStatus(stagedFileStatus)

      // ===== WHEN =====
      CatalogCommitterUtils.publishCatalogCommit(defaultEngine, logPath, catalogCommit)

      // ===== THEN =====
      val expectedPublishedFilePath = FileNames.deltaFile(logPath, 5L)
      assert(new File(expectedPublishedFilePath).exists())

      val content = scala.io.Source.fromFile(expectedPublishedFilePath).mkString
      assert(content.contains("foo"))
    }
  }

  test("publishCatalogCommit: does not overwrite when file already exists (idempotent)") {
    withTempDir { tempDir =>
      // ===== GIVEN =====
      val tablePath = tempDir.getAbsolutePath
      val logPath = s"$tablePath/_delta_log"

      val publishedPath = FileNames.deltaFile(logPath, 7L)
      writeFile(publishedPath, "foo")

      val stagedPath = FileNames.stagedCommitFile(logPath, 7L)
      writeFile(stagedPath, "bar") // in practice, staged catalog commit is identical to published

      // ===== WHEN =====
      val stagedFileStatus = defaultEngine.getFileSystemClient.getFileStatus(stagedPath)
      val catalogCommit = ParsedDeltaData.forFileStatus(stagedFileStatus)
      // We expect this to (a) not throw and (b) not overwrite the existing file
      CatalogCommitterUtils.publishCatalogCommit(defaultEngine, logPath, catalogCommit)

      // ===== THEN =====
      val content = scala.io.Source.fromFile(publishedPath).mkString
      assert(content.contains("foo"))
      assert(!content.contains("bar"))
    }
  }
}
