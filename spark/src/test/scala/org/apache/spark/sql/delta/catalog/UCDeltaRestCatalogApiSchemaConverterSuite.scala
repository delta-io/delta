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

package org.apache.spark.sql.delta.catalog

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types.{
  ArrayType,
  BooleanType,
  DecimalType,
  IntegerType,
  LongType,
  MapType,
  MetadataBuilder,
  StringType,
  StructField,
  StructType
}

class UCDeltaRestCatalogApiSchemaConverterSuite extends AnyFunSuite {

  test("converts Delta schema JSON to Spark schema") {
    val fieldMetadata = new MetadataBuilder()
      .putLong("delta.columnMapping.id", 1L)
      .putString("delta.columnMapping.physicalName", "col-1")
      .build()
    val inputSchema = StructType(Seq(
      StructField("id", LongType, nullable = false, fieldMetadata),
      StructField("amount", DecimalType(10, 2)),
      StructField("values", ArrayType(IntegerType, containsNull = true)),
      StructField("tags", MapType(StringType, BooleanType, valueContainsNull = false)),
      StructField("nested", StructType(Seq(StructField("name", StringType))))))

    val schema = UCDeltaRestCatalogApiSchemaConverter.toSparkType(inputSchema.json)

    assert(schema === inputSchema)
    assert(!schema("id").nullable)
    assert(schema("id").metadata.getLong("delta.columnMapping.id") === 1L)
    assert(schema("id").metadata.getString("delta.columnMapping.physicalName") === "col-1")
  }

  test("rejects missing schema JSON") {
    val e = intercept[IllegalArgumentException] {
      UCDeltaRestCatalogApiSchemaConverter.toSparkType(null)
    }

    assert(e.getMessage === "UC Delta Rest Catalog API table schema is missing.")
  }
}
