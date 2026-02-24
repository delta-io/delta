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
package io.delta.kernel.internal.actions

import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

import io.delta.kernel.data.{ArrayValue, ColumnVector, MapValue}
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.util.InternalUtils.singletonStringColumnVector
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.internal.util.VectorUtils.buildColumnVector
import io.delta.kernel.test.TestUtils
import io.delta.kernel.types.{IntegerType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class MetadataSuite extends AnyFunSuite with TestUtils {

  test("withMergedConfig upserts values") {
    val metadata = testMetadata(Map("a" -> "b", "f" -> "g"))

    val newMetadata = metadata.withMergedConfiguration(Map("a" -> "c", "d" -> "f").asJava)

    assert(newMetadata.getConfiguration.equals(Map("a" -> "c", "d" -> "f", "f" -> "g").asJava))
  }

  test("withReplacedConfiguration replaces values") {
    val metadata = testMetadata(Map("a" -> "b", "f" -> "g"))

    val newMetadata = metadata.withReplacedConfiguration(Map("a" -> "c", "d" -> "f").asJava)

    assert(newMetadata.getConfiguration.equals(Map("a" -> "c", "d" -> "f").asJava))
  }

  def testMetadata(tblProps: Map[String, String] = Map.empty): Metadata = {
    val testSchema = new StructType()
      .add("c1", IntegerType.INTEGER)
      .add("c2", StringType.STRING)
    new Metadata(
      "id",
      Optional.of("name"),
      Optional.of("description"),
      new Format("parquet", Collections.emptyMap()),
      testSchema.toJson,
      testSchema,
      new ArrayValue() { // partitionColumns
        override def getSize = 1

        override def getElements: ColumnVector = singletonStringColumnVector("c3")
      },
      Optional.empty(),
      new MapValue() { // conf
        override def getSize = tblProps.size
        override def getKeys: ColumnVector =
          buildColumnVector(tblProps.toSeq.map(_._1).asJava, StringType.STRING)
        override def getValues: ColumnVector =
          buildColumnVector(tblProps.toSeq.map(_._2).asJava, StringType.STRING)
      })
  }

  /**
   * Helper to create a Metadata via fromRow with a custom schemaString, bypassing eager parsing.
   * The schemaString in the row can contain unsupported types that will only fail when parsed.
   */
  private def metadataFromRowWithSchemaString(
      schemaString: String,
      tblProps: Map[String, String] = Map.empty): Metadata = {
    val formatRow = new Format("parquet", Collections.emptyMap()).toRow()
    val partitionColumns = VectorUtils.buildArrayValue(
      Collections.emptyList[String](),
      StringType.STRING)
    val configMapValue = VectorUtils.stringStringMapValue(tblProps.asJava)

    val rowValues = new java.util.HashMap[Integer, Object]()
    rowValues.put(0, "test-id")
    rowValues.put(1, null) // name
    rowValues.put(2, null) // description
    rowValues.put(3, formatRow)
    rowValues.put(4, schemaString)
    rowValues.put(5, partitionColumns)
    rowValues.put(6, null) // createdTime
    rowValues.put(7, configMapValue)

    Metadata.fromRow(new GenericRow(Metadata.FULL_SCHEMA, rowValues))
  }

  test("lazy schema parsing defers unsupported type errors") {
    // Schema JSON containing VOID type - should fail only when schema is accessed
    val voidSchemaJson =
      """{"type":"struct","fields":[{"name":"x","type":"integer","nullable":true,"metadata":{}},""" +
        """{"name":"y","type":"void","nullable":true,"metadata":{}}]}"""

    val metadata = metadataFromRowWithSchemaString(voidSchemaJson, Map("key" -> "value"))

    // Construction succeeds - no exception
    assert(metadata.getId === "test-id")
    assert(metadata.getConfiguration.get("key") === "value")
    assert(metadata.getSchemaString === voidSchemaJson)

    // getSchema() triggers lazy parsing and throws
    val e = intercept[KernelException] {
      metadata.getSchema
    }
    assert(e.getMessage.contains("Encountered unsupported Delta data type: VOID"))
  }

  test("lazy schema propagation through withReplacedConfiguration") {
    val voidSchemaJson =
      """{"type":"struct","fields":[{"name":"x","type":"integer","nullable":true,"metadata":{}},""" +
        """{"name":"y","type":"void","nullable":true,"metadata":{}}]}"""

    val metadata = metadataFromRowWithSchemaString(voidSchemaJson, Map("key" -> "value"))

    // withReplacedConfiguration should not trigger schema parsing
    val newMetadata = metadata.withReplacedConfiguration(Map("new_key" -> "new_value").asJava)

    assert(newMetadata.getId === "test-id")
    assert(newMetadata.getConfiguration.get("new_key") === "new_value")

    // getSchema() on the derived metadata should still throw
    val e = intercept[KernelException] {
      newMetadata.getSchema
    }
    assert(e.getMessage.contains("Encountered unsupported Delta data type: VOID"))
  }

  test("Metadata serialization round trip") {
    val source = testMetadata(Map("key1" -> "value1", "key2" -> "value2"))
    val deserialized = roundTripSerialize(source)

    // Verify all public methods return the same values
    assert(deserialized.getId === source.getId)
    assert(deserialized.getName === source.getName)
    assert(deserialized.getDescription === source.getDescription)
    assert(deserialized.getFormat === source.getFormat)
    assert(deserialized.getSchemaString === source.getSchemaString)
    assert(deserialized.getSchema === source.getSchema)
    assert(deserialized.getCreatedTime === source.getCreatedTime)
    assert(deserialized.getConfiguration === source.getConfiguration)
    assert(deserialized.getPartitionColNames === source.getPartitionColNames)
    assert(deserialized.getDataSchema === source.getDataSchema)
    assert(deserialized.getPhysicalSchema === source.getPhysicalSchema)

    // Verify equals and hashCode
    assert(deserialized === source)
    assert(deserialized.hashCode() === source.hashCode())
  }

}
