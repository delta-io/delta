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
package io.delta.kernel.internal.rowtracking

import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

import io.delta.kernel.internal.actions.{Format, Metadata, Protocol}
import io.delta.kernel.internal.types.DataTypeJsonSerDe
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.types.{LongType, StringType, StructField, StructType}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table

/**
 * Suite to test utility methods in RowTracking class.
 */
class RowTrackingUtilsSuite extends AnyFunSuite {

  ///////////////////////////////////////////////////////////////////////////////////////////////
  // Helper methods
  ///////////////////////////////////////////////////////////////////////////////////////////////

  private def createProtocol(writerFeatures: java.util.Set[String] =
    Collections.emptySet()): Protocol = {
    new Protocol(3, 7, Collections.emptySet(), writerFeatures)
  }

  private def createMetadata(
      configuration: Map[String, String] = Map.empty,
      schema: StructType = new StructType()): Metadata = {
    new Metadata(
      "test-id",
      Optional.empty(),
      Optional.empty(),
      new Format(),
      DataTypeJsonSerDe.serializeDataType(schema),
      schema,
      VectorUtils.buildArrayValue(Collections.emptyList(), StringType.STRING),
      Optional.empty(),
      VectorUtils.stringStringMapValue(configuration.asJava))
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////
  // Tests for RowTracking.isEnabled
  ///////////////////////////////////////////////////////////////////////////////////////////////

  test("isEnabled returns false when row tracking is not enabled") {
    val protocol = createProtocol()
    val metadata = createMetadata()

    assert(!RowTracking.isEnabled(protocol, metadata))
  }

  test("isEnabled returns true when row tracking is enabled") {
    val protocol = createProtocol(Collections.singleton("rowTracking"))
    val metadata = createMetadata(configuration = Map("delta.enableRowTracking" -> "true"))

    assert(RowTracking.isEnabled(protocol, metadata))
  }

  test("isEnabled throws error when config is enabled but protocol doesn't support it") {
    val protocol = createProtocol() // No rowTracking feature
    val metadata = createMetadata(configuration = Map("delta.enableRowTracking" -> "true"))

    val exception = intercept[IllegalStateException] {
      RowTracking.isEnabled(protocol, metadata)
    }
    assert(exception.getMessage.contains("delta.enableRowTracking"))
    assert(exception.getMessage.contains("rowTracking"))
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////
  // Tests for RowTracking.createMetadataStructFields
  ///////////////////////////////////////////////////////////////////////////////////////////////

  test("createMetadataStructFields returns empty list when row tracking is not enabled") {
    val protocol = createProtocol()
    val metadata = createMetadata()

    val fields = RowTracking.createMetadataStructFields(protocol, metadata, false, false)

    assert(fields.isEmpty)
  }

  private val nullableParameterTestCases = Table(
    (
      "testName",
      "nullableConstantFields",
      "nullableGeneratedFields",
      "expectedConstantNullable",
      "expectedGeneratedNullable"),
    ("all non-nullable", false, false, false, false),
    ("all nullable", true, true, true, true),
    ("constant nullable, generated non-nullable", true, false, true, false),
    ("constant non-nullable, generated nullable", false, true, false, true))

  test("createMetadataStructFields returns correct fields with different nullable parameters") {
    val protocol = createProtocol(Collections.singleton("rowTracking"))
    val metadata =
      createMetadata(configuration = Map("delta.enableRowTracking" -> "true"))

    forAll(nullableParameterTestCases) {
      (
          testName,
          nullableConstantFields,
          nullableGeneratedFields,
          expectedConstantNullable,
          expectedGeneratedNullable) =>
        val fields = RowTracking.createMetadataStructFields(
          protocol,
          metadata,
          nullableConstantFields,
          nullableGeneratedFields)

        val expectedFields = Seq(
          new StructField("base_row_id", LongType.LONG, expectedConstantNullable),
          new StructField(
            "default_row_commit_version",
            LongType.LONG,
            expectedConstantNullable),
          new StructField("row_id", LongType.LONG, expectedGeneratedNullable),
          new StructField("row_commit_version", LongType.LONG, expectedGeneratedNullable))

        val actualFields = fields.asScala.map(f =>
          new StructField(f.getName, f.getDataType, f.isNullable))

        assert(actualFields === expectedFields)
    }
  }
}
