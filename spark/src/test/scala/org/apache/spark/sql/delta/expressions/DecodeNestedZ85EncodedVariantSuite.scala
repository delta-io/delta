/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.expressions

import java.util.Arrays

import org.apache.spark.sql.{Column, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.variant.VariantExpressionEvalUtils
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.DeltaStatsJsonUtils
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, VariantType}
import org.apache.spark.types.variant.Variant
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

class DecodeNestedZ85EncodedVariantSuite extends QueryTest with DeltaSQLCommandTest {

  test("RoundTrip alternateVariantEncoding Z85") {
    val jsonValues = Seq(
      "21",
      "1021",
      "-29183652",
      "[1, null, true, {\"a\": 1}]",
      "{\"key1\": \"value_1\", \"key_2\": [\"value2\", 1385731029.1236421], \"key3\": false}"
    )

    jsonValues.foreach { json =>
      val inputVariant = VariantExpressionEvalUtils.parseJson(UTF8String.fromString(json))
      val variant = new Variant(inputVariant.getValue, inputVariant.getMetadata)

      // Encode as Z85
      val z85 = DeltaStatsJsonUtils.encodeVariantAsZ85(variant)

      // Create a DataFrame with the Z85 string
      val df = spark.range(1).selectExpr(s"""'{"v":"$z85"}' as z85_string""")

      // Parse as JSON (this creates a VariantVal containing the Z85 string)
      val statsSchema = StructType(Seq(StructField("v", VariantType)))
      val parsedDf = df.withColumn("parsed", from_json(col("z85_string"), statsSchema))

      // Apply DecodeNestedZ85EncodedVariant
      val decodedDf = parsedDf.withColumn(
        "decoded",
        Column(DecodeNestedZ85EncodedVariant(col("parsed").expr))
      )

      // Extract the decoded variant and verify
      val result = decodedDf.select("decoded.v").head().get(0)
      val decodedVariant = result.asInstanceOf[VariantVal]

      assert(Arrays.equals(inputVariant.getMetadata, decodedVariant.getMetadata),
        s"Metadata mismatch for JSON: $json")
      assert(Arrays.equals(inputVariant.getValue, decodedVariant.getValue),
        s"Value mismatch for JSON: $json")
    }
  }

  test("DecodeNestedZ85EncodedVariantSuite with nested struct and mixed types") {
    val json1 = "{\"id\": 100, \"name\": \"test\"}"
    val inputVariant1 = VariantExpressionEvalUtils.parseJson(UTF8String.fromString(json1))
    val variant1 = new Variant(inputVariant1.getValue, inputVariant1.getMetadata)
    val z85_1 = DeltaStatsJsonUtils.encodeVariantAsZ85(variant1)

    val json2 = "{\"count\": 42}"
    val inputVariant2 = VariantExpressionEvalUtils.parseJson(UTF8String.fromString(json2))
    val variant2 = new Variant(inputVariant2.getValue, inputVariant2.getMetadata)
    val z85_2 = DeltaStatsJsonUtils.encodeVariantAsZ85(variant2)

    // Create stats schema with nested variant, non-variant fields, and nullable variant
    val statsSchema = StructType(Seq(
      StructField("numRecords", LongType, nullable = true),
      StructField("minValues", StructType(Seq(
        StructField("intCol", IntegerType, nullable = true),
        StructField("stringCol", StringType, nullable = true),
        StructField("v", VariantType, nullable = true),
        StructField("v2", VariantType, nullable = true),
        StructField("missingField", StringType, nullable = true)
      )), nullable = true),
      StructField("maxValues", StructType(Seq(
        StructField("intCol", IntegerType, nullable = true),
        StructField("stringCol", StringType, nullable = true),
        StructField("v", VariantType, nullable = true),
        StructField("v2", VariantType, nullable = true)
      )), nullable = true)
    ))

    val statsJson = s"""{"numRecords": 1000,""" +
      s""""minValues": {"intCol": 1, "stringCol": "a", "v": "$z85_1"},""" +
      s""""maxValues": {"intCol": 100, "stringCol": "z", "v": "$z85_1", "v2": "$z85_2"}""" +
      s"""}"""

    val df = spark.range(1).selectExpr(s"""'${statsJson}' as stats""")

    val parsedDf = df.withColumn("parsed", from_json(col("stats"), statsSchema))
    val decodedDf = parsedDf.withColumn(
      "decoded",
      Column(DecodeNestedZ85EncodedVariant(col("parsed").expr))
    )

    val result = decodedDf.select(
      "decoded.numRecords",
      "decoded.minValues.intCol",
      "decoded.minValues.stringCol",
      "decoded.minValues.v",
      "decoded.minValues.v2",
      "decoded.minValues.missingField",
      "decoded.maxValues.intCol",
      "decoded.maxValues.stringCol",
      "decoded.maxValues.v",
      "decoded.maxValues.v2"
    ).head()

    // Check non-variant fields pass through unchanged
    assert(result.getLong(0) == 1000L)
    assert(result.getInt(1) == 1)
    assert(result.getString(2) == "a")

    // Check decoded variant
    val decodedVariant1Min = result.get(3).asInstanceOf[VariantVal]
    assert(Arrays.equals(inputVariant1.getMetadata, decodedVariant1Min.getMetadata))
    assert(Arrays.equals(inputVariant1.getValue, decodedVariant1Min.getValue))

    // Check null variant (v2 in minValues)
    assert(result.isNullAt(4))

    // Check missing field returns null
    assert(result.isNullAt(5))

    // Check maxValues
    assert(result.getInt(6) == 100)
    assert(result.getString(7) == "z")

    val decodedVariant1Max = result.get(8).asInstanceOf[VariantVal]
    assert(Arrays.equals(inputVariant1.getMetadata, decodedVariant1Max.getMetadata))
    assert(Arrays.equals(inputVariant1.getValue, decodedVariant1Max.getValue))

    val decodedVariant2Max = result.get(9).asInstanceOf[VariantVal]
    assert(Arrays.equals(inputVariant2.getMetadata, decodedVariant2Max.getMetadata))
    assert(Arrays.equals(inputVariant2.getValue, decodedVariant2Max.getValue))
  }

  test("fault tolerance - returns null for invalid Z85 encoded variants by default") {
    // Create a stats schema with variant fields
    val statsSchema = StructType(Seq(
      StructField("numRecords", LongType, nullable = true),
      StructField("minValues", StructType(Seq(
        StructField("intCol", IntegerType, nullable = true),
        StructField("v", VariantType, nullable = true)
      )), nullable = true)
    ))

    // Create stats JSON with an invalid Z85 string (not a valid Z85 encoding)
    // The variant field contains a non-Z85-encoded variant (just a regular JSON object)
    val invalidStatsJson =
      """{"numRecords": 100, "minValues": {"intCol": 1, "v": {"invalid": "not_z85"}}}"""

    val df = spark.range(1).selectExpr(s"""'$invalidStatsJson' as stats""")

    val parsedDf = df.withColumn("parsed", from_json(col("stats"), statsSchema))

    // By default (failOnZ85DecodeError = false), should return null for invalid variant
    val decodedDf = parsedDf.withColumn(
      "decoded",
      Column(DecodeNestedZ85EncodedVariant(col("parsed").expr))
    )

    val result = decodedDf.select(
      "decoded.numRecords",
      "decoded.minValues.intCol",
      "decoded.minValues.v"
    ).head()

    // Non-variant fields should be preserved
    assert(result.getLong(0) == 100L)
    assert(result.getInt(1) == 1)

    // Invalid variant field should be null (fault tolerant)
    assert(result.isNullAt(2), "Expected null for invalid Z85-encoded variant")
  }

  test("fault tolerance - handles corrupted Z85 string gracefully") {
    // Create a valid Z85 string and then corrupt it
    val json = "{\"id\": 42}"
    val inputVariant = VariantExpressionEvalUtils.parseJson(UTF8String.fromString(json))
    val variant = new Variant(inputVariant.getValue, inputVariant.getMetadata)
    val validZ85 = DeltaStatsJsonUtils.encodeVariantAsZ85(variant)

    // Corrupt the Z85 string by replacing some characters with invalid ones
    val corruptedZ85 = validZ85.take(5) + "!!!!!CORRUPTED!!!!!" + validZ85.drop(10)

    val statsSchema = StructType(Seq(
      StructField("v", VariantType, nullable = true)
    ))

    val statsJson = s"""{"v": "$corruptedZ85"}"""

    val df = spark.range(1).selectExpr(s"""'$statsJson' as stats""")

    val parsedDf = df.withColumn("parsed", from_json(col("stats"), statsSchema))

    // With default fault tolerance, should return null
    val decodedDf = parsedDf.withColumn(
      "decoded",
      Column(DecodeNestedZ85EncodedVariant(col("parsed").expr))
    )

    val result = decodedDf.select("decoded.v").head()

    // Corrupted Z85 should result in null (fault tolerant)
    assert(result.isNullAt(0), "Expected null for corrupted Z85 string")
  }
}
