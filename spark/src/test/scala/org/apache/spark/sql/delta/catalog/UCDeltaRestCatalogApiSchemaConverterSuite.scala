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

import java.util.Collections

import scala.collection.JavaConverters._

import io.unitycatalog.client.delta.model.{
  PrimitiveType,
  StructField => DeltaStructField,
  StructType => DeltaStructType
}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types.{
  BinaryType,
  BooleanType,
  ByteType,
  DateType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  NullType,
  ShortType,
  StringType,
  TimestampNTZType,
  TimestampType
}

class UCDeltaRestCatalogApiSchemaConverterSuite extends AnyFunSuite {

  test("converts Delta primitive type names to Spark types") {
    Seq(
      "string" -> StringType,
      "long" -> LongType,
      "integer" -> IntegerType,
      "short" -> ShortType,
      "byte" -> ByteType,
      "boolean" -> BooleanType,
      "float" -> FloatType,
      "double" -> DoubleType,
      "binary" -> BinaryType,
      "date" -> DateType,
      "timestamp" -> TimestampType,
      "timestamp_ntz" -> TimestampNTZType,
      "void" -> NullType).foreach { case (deltaType, sparkType) =>
        val schema = UCDeltaRestCatalogApiSchemaConverter.toSparkType(
          deltaSchema(deltaField(deltaType)))
        assert(schema(deltaType).dataType === sparkType)
    }
  }

  private def deltaField(name: String): DeltaStructField = {
    new DeltaStructField()
      .name(name)
      .`type`(new PrimitiveType().`type`(name))
      .nullable(true)
      .metadata(Collections.emptyMap[String, Object]())
  }

  private def deltaSchema(fields: DeltaStructField*): DeltaStructType = {
    new DeltaStructType().fields(fields.asJava)
  }
}
