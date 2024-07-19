/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.internal

import io.delta.kernel.data.{ColumnarBatch, ColumnVector, MapValue}
import io.delta.kernel.internal.actions.{Format, Metadata}
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.internal.util.VectorUtils.{stringArrayValue, stringVector}
import io.delta.kernel.test.{BaseMockJsonHandler, MockEngineUtils}
import io.delta.kernel.types.{DataType, MapType, StringType, StructType}
import org.scalatest.funsuite.AnyFunSuite

import java.util.{Collections, Optional}
import scala.collection.JavaConverters._

class TableConfigSuite extends AnyFunSuite with MockEngineUtils {
  def getEmptyMetadata: Metadata = {
    new Metadata(
      java.util.UUID.randomUUID().toString,
      Optional.empty(),
      Optional.empty(),
      new Format(),
      "",
      null,
      stringArrayValue(Collections.emptyList()),
      Optional.empty(),
      VectorUtils.stringStringMapValue(Collections.emptyMap())
    )
  }

  test("Coordinated Commit Related Properties from Metadata") {
    val expMap = Map("key1" -> "string_value", "key2Int" -> "2", "key3ComplexStr" -> "\"hello\"")
    val engine1 = mockEngine(jsonHandler = new KeyValueJsonHandler(expMap))
    val m1 = getEmptyMetadata.withNewConfiguration(
      Map(
        TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "cc-name",
        TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey ->
          """{"key1": "string_value", "key2Int": "2", "key3ComplexStr": "\"hello\""}""",
        TableConfig.COORDINATED_COMMITS_TABLE_CONF.getKey ->
          """{"key1": "string_value", "key2Int": "2", "key3ComplexStr": "\"hello\""}""").asJava
    )
    assert(TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.fromMetadata(engine1, m1) ===
      Optional.of("cc-name"))

    assert(TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.fromMetadata(engine1, m1) ===
      expMap.asJava)

    assert(TableConfig.COORDINATED_COMMITS_TABLE_CONF.fromMetadata(engine1, m1) ===
      expMap.asJava)

    val engine2 = mockEngine(jsonHandler = new RuntimeExceptionJsonHandler)
    val m2 = getEmptyMetadata.withNewConfiguration(
      Map(
        TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> null,
        TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey ->
          """{"key1": "string_value", "key2Int": "2""",
        TableConfig.COORDINATED_COMMITS_TABLE_CONF.getKey ->
          """{"key1": "string_value", "key2Int": "2""").asJava
    )
    assert(TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.fromMetadata(engine2, m2) ===
      Optional.empty())
    intercept[RuntimeException] {
      TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.fromMetadata(engine2, m2)
    }
    intercept[RuntimeException] {
      TableConfig.COORDINATED_COMMITS_TABLE_CONF.fromMetadata(engine2, m2)
    }
  }
}

class KeyValueJsonHandler(map: Map[String, String]) extends BaseMockJsonHandler {
  val schema: StructType =
    new StructType().add("config", new MapType(StringType.STRING, StringType.STRING, true))
  override def parseJson(
    jsonStringVector: ColumnVector,
    outputSchema: StructType,
    selectionVector: Optional[ColumnVector]): ColumnarBatch = {
    new ColumnarBatch {
      override def getSchema: StructType = schema

      override def getColumnVector(ordinal: Int): ColumnVector = {
        ordinal match {
          case 0 => mapTypeVector(map.keys.toSeq, map.values.toSeq) // path
        }
      }

      override def getSize: Int = 1
    }
  }

  def mapTypeVector(keys: Seq[String], values: Seq[String]): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = new MapType(StringType.STRING, StringType.STRING, true)

      override def getSize: Int = 1

      override def close(): Unit = {}

      override def isNullAt(rowId: Int): Boolean = rowId >= 1

      override def getMap(rowId: Int): MapValue = new MapValue {

        override def getSize: Int = keys.size

        override def getKeys: ColumnVector = stringVector(keys.asJava)

        override def getValues: ColumnVector = stringVector(values.asJava)
      }
    }
  }
}

class RuntimeExceptionJsonHandler extends BaseMockJsonHandler {
  override def parseJson(
    jsonStringVector: ColumnVector,
    outputSchema: StructType,
    selectionVector: Optional[ColumnVector]): ColumnarBatch = throw new RuntimeException()
}
