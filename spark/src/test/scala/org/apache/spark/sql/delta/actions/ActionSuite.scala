/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.actions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}

class ActionSuite extends SparkFunSuite with SharedSparkSession {

  test("CheckpointMetadata - extract from tags") {
    val metadata1 = CheckpointMetadata(version = 1, tags = null)
    assert(metadata1.numOfAddFiles.isEmpty)
    assert(metadata1.sidecarSizeInBytes.isEmpty)
    assert(metadata1.sidecarNumActions.isEmpty)
    assert(metadata1.sidecarFileSchema.isEmpty)

    import CheckpointMetadata.Tags
    val metadata2 = CheckpointMetadata(
      version = 1,
      tags = Map(Tags.NUM_OF_ADD_FILES.name -> "2", Tags.SIDECAR_SIZE_IN_BYTES.name -> "3"))
    assert(metadata2.numOfAddFiles.contains(2))
    assert(metadata2.sidecarSizeInBytes.contains(3))
    assert(metadata2.sidecarNumActions.isEmpty)
    assert(metadata2.sidecarFileSchema.isEmpty)

    val schemaForMetadata = StructType(Seq(StructField("a", dataType = BooleanType)))
    val metadata3 = CheckpointMetadata(
      version = 1,
      tags = Map(
        Tags.SIDECAR_NUM_ACTIONS.name -> "2",
        Tags.SIDECAR_FILE_SCHEMA.name -> schemaForMetadata.json)
    )
    assert(metadata3.numOfAddFiles.isEmpty)
    assert(metadata3.sidecarSizeInBytes.isEmpty)
    assert(metadata3.sidecarNumActions.contains(2))
    assert(metadata3.sidecarFileSchema.contains(schemaForMetadata))
  }
}
