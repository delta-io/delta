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
    val origin = new DeltaCommittable("jobId", "operatorId", 100, actions)
    val serde = new DeltaCommittable.Serializer
    val deserialized = serde.deserialize(1, serde.serialize(origin))

    assert(deserialized.getJobId == origin.getJobId)
    assert(deserialized.getOperatorId == origin.getOperatorId)
    assert(deserialized.getCheckpointId == origin.getCheckpointId)
    assert(deserialized.getDeltaActions.asScala.map(JsonUtils.rowToJson)
      == actions.asScala.map(JsonUtils.rowToJson))
  }
}
