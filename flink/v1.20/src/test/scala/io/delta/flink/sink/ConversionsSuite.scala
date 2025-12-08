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
