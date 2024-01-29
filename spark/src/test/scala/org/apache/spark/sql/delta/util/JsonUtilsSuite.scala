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

package org.apache.spark.sql.delta.util

import scala.util.Random

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.delta.stats.DataSize
import com.fasterxml.jackson.core.StreamReadConstraints

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

class JsonUtilsSuite
  extends QueryTest
  with SharedSparkSession {

  test("DataSize json serialization") {
    val testCases = Seq(
      DataSize() -> """{}""",
      DataSize(bytesCompressed = Some(816L)) -> """{"bytesCompressed":816}""",
      DataSize(rows = Some(111L)) -> """{"rows":111}""",
      DataSize(logicalRows = Some(111L)) -> """{"logicalRows":111}""",
      DataSize(bytesCompressed = Some(816L), rows = Some(111L), logicalRows = Some(111L)) ->
        """{"bytesCompressed":816,"rows":111,"logicalRows":111}"""
    )
    for ((obj, json) <- testCases) {
      assert(JsonUtils.toJson(obj) == json)
      assert(JsonUtils.fromJson[DataSize](json) == obj)
    }
  }

  test("Serialize and de-serialize commit info with large message") {
    val operationStringSize = StreamReadConstraints.DEFAULT_MAX_STRING_LEN * 10
    assert(operationStringSize > StreamReadConstraints.DEFAULT_MAX_STRING_LEN)

    val operation = Random.alphanumeric.take(operationStringSize).toString()
    val commitInfo = CommitInfo(
      time = System.currentTimeMillis(),
      operation,
      operationParameters = Map.empty,
      commandContext = Map.empty,
      readVersion = Some(1),
      isolationLevel = None,
      isBlindAppend = Some(false),
      operationMetrics = None,
      userMetadata = Some("I am a test and not a user"),
      tags = None,
      txnId = Some("Transaction with a veryyyyyyy large commit info")
    )

    val serialized = JsonUtils.toJson(commitInfo)
    val deserialized = JsonUtils.fromJson[CommitInfo](serialized)

    assert(commitInfo === deserialized)
  }
}
