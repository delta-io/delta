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

import io.delta.kernel.data.{ColumnarBatch, ColumnVector, MapValue, Row}
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.util.VectorUtils.{stringArrayValue, stringVector}
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.test.{BaseMockJsonHandler, MockEngineUtils}
import io.delta.kernel.types.{DataType, MapType, StringType, StructType}
import io.delta.kernel.utils.CloseableIterator
import org.scalatest.funsuite.AnyFunSuite

import java.io.IOException
import java.util.{Collections, Optional}
import scala.collection.JavaConverters._

class TableConfigSuite extends AnyFunSuite with MockEngineUtils {

  test("Parse Map[String, String] type table config") {
    val expMap = Map("key1" -> "string_value", "key2Int" -> "2", "key3ComplexStr" -> "\"hello\"")
    val engine = mockEngine(jsonHandler = new KeyValueJsonHandler(expMap))
    val input = """{"key1": "string_value", "key2Int": "2", "key3ComplexStr": "\"hello\""}"""
    assert(TableConfig.parseJSONKeyValueMap(engine, input) === expMap.asJava)

    val engine1 = mockEngine(jsonHandler = new ReturnNullRowsJsonHandler(nRow = 0))
    val e1 = intercept[IllegalStateException] {
      TableConfig.parseJSONKeyValueMap(engine1, """{"key": "value"}""")
    }
    assert(e1.getMessage.contains("""Unable to parse {"key": "value"}"""))

    val engine2 = mockEngine(jsonHandler = new ReturnNullRowsJsonHandler(nRow = 2))
    val e2 = intercept[IllegalArgumentException] {
      TableConfig.parseJSONKeyValueMap(engine2, """{"key": "value"}""")
    }
    assert(e2.getMessage.contains("Iterator contains more than one element"))

    val errMsg = "Close called failed"
    val engine3 = mockEngine(jsonHandler = new ReturnCloseFailIteratorJsonHandler(errMsg = errMsg))
    val e3 = intercept[KernelException] {
      TableConfig.parseJSONKeyValueMap(engine3, """{"key": "value"}""")
    }
    assert(e3.getMessage.contains(s"java.io.IOException: $errMsg"))

    val engine4 = mockEngine(jsonHandler = new ReturnNullRowsJsonHandler(nRow = 1))
    val e4 = intercept[IllegalArgumentException] {
      TableConfig.parseJSONKeyValueMap(engine4, """{"key": "value"}""")
    }
    assert(e4.getMessage === null)
  }

  test("check TableConfig.editable is true") {
    val engine = mockEngine(jsonHandler = new KeyValueJsonHandler(Map()))

    TableConfig.validateProperties(engine,
      Map(
        TableConfig.TOMBSTONE_RETENTION.getKey -> "interval 2 week",
        TableConfig.CHECKPOINT_INTERVAL.getKey -> "20",
        TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true",
        TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.getKey -> "1",
        TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.getKey -> "1",
        TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "{in-memory}",
        TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{\"1\": \"1\"}",
        TableConfig.COORDINATED_COMMITS_TABLE_CONF.getKey -> "{\"1\": \"1\"}",
        TableConfig.COLUMN_MAPPING_MODE.getKey -> "name",
        TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true").asJava)
  }

  test("check TableConfig.MAX_COLUMN_ID.editable is false") {
    val engine = mockEngine(jsonHandler = new KeyValueJsonHandler(Map()))

    val e = intercept[KernelException] {
      TableConfig.validateProperties(engine,
        Map(
          TableConfig.TOMBSTONE_RETENTION.getKey -> "interval 2 week",
          TableConfig.CHECKPOINT_INTERVAL.getKey -> "20",
          TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true",
          TableConfig.MAX_COLUMN_ID.getKey -> "10").asJava)
    }

    assert(e.isInstanceOf[KernelException])
    assert(e.getMessage.toLowerCase() ==
      s"The Delta table property '${TableConfig.MAX_COLUMN_ID.getKey}'".toLowerCase() +
      s" is an internal property and cannot be updated.".toLowerCase())
  }
}

/**
 * Mock JsonHandler which returns a ColumnarBatch with a single row containing a Map[String, String]
 * column.
 */
class KeyValueJsonHandler(
  map: Map[String, String]) extends BaseMockJsonHandler with VectorTestUtils {
  val expSchema: StructType =
    new StructType().add("config", new MapType(StringType.STRING, StringType.STRING, true))
  override def parseJson(
    jsonStringVector: ColumnVector,
    outputSchema: StructType,
    selectionVector: Optional[ColumnVector]): ColumnarBatch = {
    assert(outputSchema == expSchema)
    new ColumnarBatch {
      override def getSchema: StructType = outputSchema

      override def getColumnVector(ordinal: Int): ColumnVector = {
        ordinal match {
          case 0 => mapTypeVector(Seq(map))
        }
      }

      override def getSize: Int = 1
    }
  }
}

/**
 * Mock JsonHandler which returns a ColumnarBatch with nRow rows containing a single null column.
 */
class ReturnNullRowsJsonHandler(nRow: Int) extends BaseMockJsonHandler {
  override def parseJson(
    jsonStringVector: ColumnVector,
    outputSchema: StructType,
    selectionVector: Optional[ColumnVector]): ColumnarBatch = {
    new ColumnarBatch {
      override def getSchema: StructType = outputSchema

      override def getColumnVector(ordinal: Int): ColumnVector = {
        stringVector(List.fill(nRow)(null).asInstanceOf[List[String]].asJava)
      }

      override def getSize: Int = nRow
    }
  }
}

/**
 * Mock JsonHandler which returns a ColumnarBatch with an iterator that throws an IOException when
 * close is called.
 */
class ReturnCloseFailIteratorJsonHandler(errMsg: String) extends BaseMockJsonHandler {
  override def parseJson(
    jsonStringVector: ColumnVector,
    outputSchema: StructType,
    selectionVector: Optional[ColumnVector]): ColumnarBatch = {
    new ColumnarBatch {
      override def getSchema: StructType = outputSchema

      override def getColumnVector(ordinal: Int): ColumnVector = null

      override def getSize: Int = 1

      override def getRows: CloseableIterator[Row] = {
        new CloseableIterator[Row] {
          var rowId = 0

          override def hasNext: Boolean = rowId < 1

          override def next(): Row = {
            rowId += 1
            null
          }

          override def close(): Unit = {
            throw new IOException(errMsg)
          }
        }
      }
    }
  }
}
