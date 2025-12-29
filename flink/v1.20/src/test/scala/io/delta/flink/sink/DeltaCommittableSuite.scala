/*
 *  Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.flink.sink

import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}

import io.delta.flink.TestHelper
import io.delta.kernel.defaults.internal.json.JsonUtils
import io.delta.kernel.expressions.Literal
import io.delta.kernel.types.{IntegerType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class DeltaCommittableSuite extends AnyFunSuite with TestHelper {

  test("serialize and deserialize") {
    val schema = new StructType().add("id", IntegerType.INTEGER)
      .add("part", StringType.STRING)
    val actions = (1 to 10)
      .map(value =>
        dummyAddFileRow(schema, 10, partitionValues = Map("part" -> Literal.ofInt(value))))
      .toList.asJava
    val origin = new DeltaCommittable(
      "jobId",
      "operatorId",
      100,
      actions,
      new WriterResultContext())
    val serde = new DeltaCommittable.Serializer
    val deserialized = serde.deserialize(1, serde.serialize(origin))

    assert(deserialized.getJobId == origin.getJobId)
    assert(deserialized.getOperatorId == origin.getOperatorId)
    assert(deserialized.getCheckpointId == origin.getCheckpointId)
    assert(deserialized.getDeltaActions.asScala.map(JsonUtils.rowToJson)
      == actions.asScala.map(JsonUtils.rowToJson))
  }
}
