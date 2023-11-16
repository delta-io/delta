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
package io.delta.kernel.defaults.client

import scala.collection.JavaConverters._

import org.scalatest.funsuite.AnyFunSuite
import io.delta.kernel.types._
import org.apache.hadoop.conf.Configuration

// NOTE: currently tests are split across scala and java; additional tests are in
// TestDefaultJsonHandler.java
class DefaultJsonHandlerSuite extends AnyFunSuite {

  val jsonHandler = new DefaultJsonHandler(new Configuration());

  //////////////////////////////////////////////////////////////////////////////////
  // END-TO-END TESTS FOR deserializeStructType (more tests in DataTypeParserSuite)
  //////////////////////////////////////////////////////////////////////////////////

  // TODO once we add full support for field metadata update this to include other types
  private def sampleMetadata: java.util.Map[String, String] =
    Map(
      "key1" -> "value1",
      "key2" -> "value2"
    ).asJava

  test("deserializeStructType: primitive type round trip") {
    val fields = BasePrimitiveType.getAllPrimitiveTypes().asScala.flatMap { dataType =>
      Seq(
        new StructField("col1" + dataType, dataType, true),
        new StructField("col1" + dataType, dataType, false),
        new StructField("col1" + dataType, dataType, false, sampleMetadata)
      )
    } ++ Seq(
      new StructField("col1decimal", new DecimalType(30, 10), true),
      new StructField("col2decimal", new DecimalType(38, 22), true),
      new StructField("col3decimal", new DecimalType(5, 2), true)
    )

    val expSchema = new StructType(fields.asJava);
    val serializedSchema = expSchema.toJson
    val actSchema = jsonHandler.deserializeStructType(serializedSchema)
    assert(expSchema == actSchema)
  }

  test("deserializeStructType: complex type round trip") {
    val arrayType = new ArrayType(IntegerType.INTEGER, true)
    val arrayArrayType = new ArrayType(arrayType, false)
    val mapType = new MapType(FloatType.FLOAT, BinaryType.BINARY, false)
    val mapMapType = new MapType(mapType, BinaryType.BINARY, true)
    val structType = new StructType().add("simple", DateType.DATE)
    val structAllType = new StructType()
      .add("prim", BooleanType.BOOLEAN)
      .add("arr", arrayType)
      .add("map", mapType)
      .add("struct", structType)

    val expSchema = new StructType()
      .add("col1", arrayType, true)
      .add("col2", arrayArrayType, false)
      .add("col3", mapType, false)
      .add("col4", mapMapType, false)
      .add("col5", structType, false)
      .add("col6", structAllType, false)

    val serializedSchema = expSchema.toJson
    val actSchema = jsonHandler.deserializeStructType(serializedSchema)
    assert(expSchema == actSchema)
  }

  test("deserializeStructType: not a StructType") {
    val e = intercept[IllegalArgumentException] {
      jsonHandler.deserializeStructType(new ArrayType(StringType.STRING, true).toJson())
    }
    assert(e.getMessage.contains("Could not parse the following JSON as a valid StructType"))
  }

  test("deserializeStructType: invalid JSON") {
    val e = intercept[RuntimeException] {
      jsonHandler.deserializeStructType(
        """
          |{
          |  "type" : "struct,
          |  "fields" : []
          |}
          |""".stripMargin
      )
    }
    assert(e.getMessage.contains("Could not parse JSON"))
  }

  // TODO we use toJson to serialize our physical and logical schemas in ScanStateRow, we should
  //  test DataType.toJson
}
