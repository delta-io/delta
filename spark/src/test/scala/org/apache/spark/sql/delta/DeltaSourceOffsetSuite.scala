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

package org.apache.spark.sql.delta

import java.util.UUID

import org.apache.spark.sql.delta.sources.DeltaSourceOffset
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.SparkFunSuite
import org.apache.spark.SparkThrowable
import org.apache.spark.sql.execution.streaming.SerializedOffset

class DeltaSourceOffsetSuite extends SparkFunSuite {

  test("unknown sourceVersion value") {
    // Set unknown sourceVersion as the max allowed version plus 1.
    val unknownVersion = 4

    // Note: "isStartingVersion" corresponds to DeltaSourceOffset.isInitialSnapshot.
    val json =
      s"""
         |{
         |  "sourceVersion": $unknownVersion,
         |  "reservoirVersion": 1,
         |  "index": 1,
         |  "isStartingVersion": true
         |}
      """.stripMargin
    val e = intercept[SparkThrowable] {
      DeltaSourceOffset(
        UUID.randomUUID().toString,
        SerializedOffset(json))
    }
    assert(e.getErrorClass == "DELTA_INVALID_FORMAT_FROM_SOURCE_VERSION")
    assert(e.toString.contains("Please upgrade to newer version of Delta"))
  }

  test("invalid sourceVersion value") {
    // Note: "isStartingVersion" corresponds to DeltaSourceOffset.isInitialSnapshot.
    val json =
      """
        |{
        |  "sourceVersion": "foo",
        |  "reservoirVersion": 1,
        |  "index": 1,
        |  "isStartingVersion": true
        |}
      """.stripMargin
    val e = intercept[SparkThrowable] {
      DeltaSourceOffset(
        UUID.randomUUID().toString,
        SerializedOffset(json))
    }
    assert(e.getErrorClass == "DELTA_INVALID_SOURCE_OFFSET_FORMAT")
    assert(e.toString.contains("source offset format is invalid"))
  }

  test("missing sourceVersion") {
    // Note: "isStartingVersion" corresponds to DeltaSourceOffset.isInitialSnapshot.
    val json =
      """
        |{
        |  "reservoirVersion": 1,
        |  "index": 1,
        |  "isStartingVersion": true
        |}
      """.stripMargin
    val e = intercept[SparkThrowable] {
      DeltaSourceOffset(
        UUID.randomUUID().toString,
        SerializedOffset(json))
    }
    assert(e.getErrorClass == "DELTA_INVALID_SOURCE_VERSION")
    for (msg <- "is invalid") {
      assert(e.toString.contains(msg))
    }
  }

  test("unmatched reservoir id") {
    // Note: "isStartingVersion" corresponds to DeltaSourceOffset.isInitialSnapshot.
    val json =
      s"""
        |{
        |  "reservoirId": "${UUID.randomUUID().toString}",
        |  "sourceVersion": 1,
        |  "reservoirVersion": 1,
        |  "index": 1,
        |  "isStartingVersion": true
        |}
      """.stripMargin
    val e = intercept[SparkThrowable] {
      DeltaSourceOffset(
        UUID.randomUUID().toString,
        SerializedOffset(json))
    }
    assert(e.getErrorClass == "DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE")
    for (msg <- Seq("delete", "checkpoint", "restart")) {
      assert(e.toString.contains(msg))
    }
  }

  test("isInitialSnapshot serializes as isStartingVersion") {
    for (isStartingVersion <- Seq(false, true)) {
      // From serialized to object
      val reservoirId = UUID.randomUUID().toString
      val json =
        s"""
           |{
           |  "reservoirId": "$reservoirId",
           |  "sourceVersion": 1,
           |  "reservoirVersion": 1,
           |  "index": 1,
           |  "isStartingVersion": $isStartingVersion
           |}
      """.stripMargin
      val offsetDeserialized = DeltaSourceOffset(reservoirId, SerializedOffset(json))
      assert(offsetDeserialized.isInitialSnapshot === isStartingVersion)

      // From object to serialized
      val offset = DeltaSourceOffset(
        reservoirId = reservoirId,
        reservoirVersion = 7,
        index = 13,
        isInitialSnapshot = isStartingVersion)
      assert(offset.json.contains(s""""isStartingVersion":$isStartingVersion"""))
    }
  }

  test("DeltaSourceOffset deserialization") {
    // Source version 1 with BASE_INDEX_V1
    val reservoirId = UUID.randomUUID().toString
    val jsonV1 =
      s"""
         |{
         |  "reservoirId": "$reservoirId",
         |  "sourceVersion": 1,
         |  "reservoirVersion": 3,
         |  "index": -1,
         |  "isStartingVersion": false
         |}
    """.stripMargin
    val offsetDeserializedV1 = JsonUtils.fromJson[DeltaSourceOffset](jsonV1)
    assert(offsetDeserializedV1 ==
      DeltaSourceOffset(reservoirId, 3, DeltaSourceOffset.BASE_INDEX, false))

    // Source version 3 with BASE_INDEX_V3
    val jsonV3 =
      s"""
         |{
         |  "reservoirId": "$reservoirId",
         |  "sourceVersion": 3,
         |  "reservoirVersion": 7,
         |  "index": -100,
         |  "isStartingVersion": false
         |}
    """.stripMargin
    val offsetDeserializedV3 = JsonUtils.fromJson[DeltaSourceOffset](jsonV3)
    assert(offsetDeserializedV3 ==
      DeltaSourceOffset(reservoirId, 7, DeltaSourceOffset.BASE_INDEX, false))

    // Source version 3 with METADATA_CHANGE_INDEX
    val jsonV3metadataChange =
      s"""
         |{
         |  "reservoirId": "$reservoirId",
         |  "sourceVersion": 3,
         |  "reservoirVersion": 7,
         |  "index": -20,
         |  "isStartingVersion": false
         |}
    """.stripMargin
    val offsetDeserializedV3metadataChange =
      JsonUtils.fromJson[DeltaSourceOffset](jsonV3metadataChange)
    assert(offsetDeserializedV3metadataChange ==
      DeltaSourceOffset(reservoirId, 7, DeltaSourceOffset.METADATA_CHANGE_INDEX, false))

    // Source version 3 with regular index and isStartingVersion = true
    val jsonV3start =
      s"""
         |{
         |  "reservoirId": "$reservoirId",
         |  "sourceVersion": 3,
         |  "reservoirVersion": 9,
         |  "index": 23,
         |  "isStartingVersion": true
         |}
    """.stripMargin
    val offsetDeserializedV3start = JsonUtils.fromJson[DeltaSourceOffset](jsonV3start)
    assert(offsetDeserializedV3start == DeltaSourceOffset(reservoirId, 9, 23, true))
  }

  test("DeltaSourceOffset deserialization error") {
    val reservoirId = UUID.randomUUID().toString
    // This is missing a double quote so it's unbalanced.
    val jsonV1 =
      s"""
         |{
         |  "reservoirId": "$reservoirId",
         |  "sourceVersion": 23x,
         |  "reservoirVersion": 3,
         |  "index": -1,
         |  "isStartingVersion": false
         |}
    """.stripMargin
    val e = intercept[SparkThrowable] {
      JsonUtils.fromJson[DeltaSourceOffset](jsonV1)
    }
    assert(e.getErrorClass == "DELTA_INVALID_SOURCE_OFFSET_FORMAT")
  }

  test("DeltaSourceOffset serialization") {
    val reservoirId = UUID.randomUUID().toString
    // BASE_INDEX is always serialized as V1.
    val offsetV1 = DeltaSourceOffset(reservoirId, 3, DeltaSourceOffset.BASE_INDEX, false)
    assert(JsonUtils.toJson(offsetV1) ===
      s"""{"sourceVersion":1,"reservoirId":"$reservoirId","reservoirVersion":3,"index":-1,""" +
      s""""isStartingVersion":false}""")
    // The same serializer should be used by both methods.
    assert(JsonUtils.toJson(offsetV1) === offsetV1.json)

    // METADATA_CHANGE_INDEX is always serialized as V3
    val offsetV3metadataChange =
      DeltaSourceOffset(reservoirId, 7, DeltaSourceOffset.METADATA_CHANGE_INDEX, false)
    assert(JsonUtils.toJson(offsetV3metadataChange) ===
      s"""{"sourceVersion":3,"reservoirId":"$reservoirId","reservoirVersion":7,"index":-20,""" +
      s""""isStartingVersion":false}""")
    // The same serializer should be used by both methods.
    assert(JsonUtils.toJson(offsetV3metadataChange) === offsetV3metadataChange.json)

    // Regular index and isStartingVersion = true, serialized as V1
    val offsetV1start = DeltaSourceOffset(reservoirId, 9, 23, true)
    assert(JsonUtils.toJson(offsetV1start) ===
      s"""{"sourceVersion":1,"reservoirId":"$reservoirId","reservoirVersion":9,"index":23,""" +
      s""""isStartingVersion":true}""")
    // The same serializer should be used by both methods.
    assert(JsonUtils.toJson(offsetV1start) === offsetV1start.json)
  }

  test("DeltaSourceOffset.validateOffsets") {
    DeltaSourceOffset.validateOffsets(
      previousOffset = DeltaSourceOffset(
        reservoirId = "foo",
        reservoirVersion = 4,
        index = 10,
        isInitialSnapshot = false),
      currentOffset = DeltaSourceOffset(
        reservoirId = "foo",
        reservoirVersion = 4,
        index = 10,
        isInitialSnapshot = false))
    DeltaSourceOffset.validateOffsets(
      previousOffset = DeltaSourceOffset(
        reservoirId = "foo",
        reservoirVersion = 4,
        index = 10,
        isInitialSnapshot = false),
      currentOffset = DeltaSourceOffset(
        reservoirId = "foo",
        reservoirVersion = 5,
        index = 1,
        isInitialSnapshot = false))

    assert(intercept[IllegalStateException] {
      DeltaSourceOffset.validateOffsets(
        previousOffset = DeltaSourceOffset(
          reservoirId = "foo",
          reservoirVersion = 4,
          index = 10,
          isInitialSnapshot = false),
        currentOffset = DeltaSourceOffset(
          reservoirId = "foo",
          reservoirVersion = 4,
          index = 10,
          isInitialSnapshot = true))
    }.getMessage.contains("Found invalid offsets: 'isInitialSnapshot' flipped incorrectly."))
    assert(intercept[IllegalStateException] {
      DeltaSourceOffset.validateOffsets(
        previousOffset = DeltaSourceOffset(
          reservoirId = "foo",
          reservoirVersion = 4,
          index = 10,
          isInitialSnapshot = false),
        currentOffset = DeltaSourceOffset(
          reservoirId = "foo",
          reservoirVersion = 1,
          index = 10,
          isInitialSnapshot = false))
    }.getMessage.contains("Found invalid offsets: 'reservoirVersion' moved back."))
    assert(intercept[IllegalStateException] {
      DeltaSourceOffset.validateOffsets(
        previousOffset = DeltaSourceOffset(
          reservoirId = "foo",
          reservoirVersion = 4,
          index = 10,
          isInitialSnapshot = false),
        currentOffset = DeltaSourceOffset(
          reservoirId = "foo",
          reservoirVersion = 4,
          index = 9,
          isInitialSnapshot = false))
    }.getMessage.contains("Found invalid offsets. 'index' moved back."))
  }
}
