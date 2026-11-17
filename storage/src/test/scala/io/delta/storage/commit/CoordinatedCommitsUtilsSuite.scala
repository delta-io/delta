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

package io.delta.storage.commit

import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

class CoordinatedCommitsUtilsSuite extends AnyFunSuite {

  private val logPath = new Path("s3://bucket/table/_delta_log")

  test("catalogCommitPath: staged file name resolves under _staged_commits") {
    Seq("00000000000000000005.uuid-1234.json", "5.uuid.json").foreach { fileName =>
      assert(CoordinatedCommitsUtils.catalogCommitPath(logPath, 5, fileName) ===
        new Path(s"s3://bucket/table/_delta_log/_staged_commits/$fileName"))
    }
  }

  test("catalogCommitPath: published file name resolves under _delta_log") {
    Seq("00000000000000000005.json", "5.json").foreach { fileName =>
      assert(CoordinatedCommitsUtils.catalogCommitPath(logPath, 5, fileName) ===
        new Path(s"s3://bucket/table/_delta_log/$fileName"))
    }
  }

  test("catalogCommitPath: rejects non-commit file names") {
    Seq(
      "00000000000000000005.crc",
      "00000000000000000005.checkpoint.parquet",
      "foo.json",
      "_staged_commits/00000000000000000005.json",
      "00000000000000000005.a/b.json",
      "/abs/00000000000000000005.json",
      "").foreach { fileName =>
      val ex = intercept[IllegalArgumentException] {
        CoordinatedCommitsUtils.catalogCommitPath(logPath, 5, fileName)
      }
      assert(ex.getMessage.contains("Expected a commit file name but got"))
    }
  }

  test("catalogCommitPath: rejects null file name") {
    val ex = intercept[IllegalArgumentException] {
      CoordinatedCommitsUtils.catalogCommitPath(logPath, 5, null)
    }
    assert(ex.getMessage.contains("must not be null"))
  }

  test("catalogCommitPath: rejects version mismatch") {
    Seq("00000000000000000005.json", "00000000000000000005.uuid.json").foreach { fileName =>
      val ex = intercept[IllegalArgumentException] {
        CoordinatedCommitsUtils.catalogCommitPath(logPath, 6, fileName)
      }
      assert(ex.getMessage.contains("has version 5 but the catalog reported version 6"))
    }
  }

  test("catalogCommitPath: rejects negative version") {
    val ex = intercept[IllegalArgumentException] {
      CoordinatedCommitsUtils.catalogCommitPath(logPath, -1, "00000000000000000000.json")
    }
    assert(ex.getMessage.contains("must be non-negative"))
  }
}
