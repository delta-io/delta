package io.delta.flink.sink

import io.delta.kernel.types.{IntegerType, StringType, StructType}

import org.apache.flink.table.types.logical.{IntType, RowType, VarCharType}
import org.scalatest.funsuite.AnyFunSuite

class ConversionsSuite extends AnyFunSuite {

  test("convert simple schema") {
    val flinkSchema = RowType.of(
      Array(new IntType(), new VarCharType(VarCharType.MAX_LENGTH)),
      Array[String]("id", "part"))

    val deltaSchema = new StructType()
      .add("id", IntegerType.INTEGER)
      .add("part", StringType.STRING)
    assert(Conversions.FlinkToDelta.schema(flinkSchema).equivalent(deltaSchema))
  }

  test("convert struct") {}

  test("convert list") {}

  test("convert map") {}
}
