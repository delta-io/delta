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
import io.delta.kernel.internal.DeltaLogActionUtils.listDeltaLogFiles
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames

import org.scalatest.funsuite.AnyFunSuite

/** Test suite for end-to-end cases. See also the mocked unit tests in DeltaLogActionUtilsSuite. */
class DeltaLogActionUtilsE2ESuite extends AnyFunSuite with TestUtils {
  test("listDeltaLogFiles: throws TableNotFoundException if _delta_log does not exist") {
    withTempDir { tableDir =>
      intercept[TableNotFoundException] {
        listDeltaLogFiles(
          defaultEngine,
          Set(FileNames.DeltaLogFileType.COMMIT, FileNames.DeltaLogFileType.CHECKPOINT).asJava,
          new Path(tableDir.getAbsolutePath),
          0,
          Optional.empty(),
          true /* mustBeRecreatable */
        )
      }
    }
  }

  test("listDeltaLogFiles: returns empty list if _delta_log is empty") {
    withTempDir { tableDir =>
      val logDir = new File(tableDir, "_delta_log")
      assert(logDir.mkdirs() && logDir.isDirectory && logDir.listFiles().isEmpty)

      val result = listDeltaLogFiles(
        defaultEngine,
        Set(FileNames.DeltaLogFileType.COMMIT, FileNames.DeltaLogFileType.CHECKPOINT).asJava,
        new Path(tableDir.getAbsolutePath),
        0,
        Optional.empty(),
        true /* mustBeRecreatable */
      )

      assert(result.isEmpty)
    }
  }
}
