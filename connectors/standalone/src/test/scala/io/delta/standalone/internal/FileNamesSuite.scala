/*
 * Copyright (2024-present) The Delta Lake Project Authors.
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

package io.delta.standalone.internal

// scalastyle:off funsuite
import io.delta.standalone.internal.util.FileNames
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite

class FileNamesSuite extends FunSuite {

  test("multiPartCheckpointPart") {
    assert(
      FileNames.multiPartCheckpointPart(
        new Path("00000000000000004915.checkpoint.0000000020.0000000060.parquet")
      ).contains(20)
    )

    assert(
      FileNames.multiPartCheckpointPart(
        new Path("00000000000000000010.checkpoint.parquet")
      ).isEmpty
    )
  }

}
// scalastyle:on funsuite
