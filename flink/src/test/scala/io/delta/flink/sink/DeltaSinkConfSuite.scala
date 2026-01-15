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

import scala.jdk.CollectionConverters.MapHasAsJava

import io.delta.kernel.internal.util.ColumnMapping
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class DeltaSinkConfSuite extends AnyFunSuite {

  test("schema evolution mode allow without physical name") {
    val schema = new StructType()
      .add("id", IntegerType.INTEGER)
      .add("name", StringType.STRING)
    val conf = new DeltaSinkConf(
      schema,
      Map(DeltaSinkConf.SCHEMA_EVOLUTION_MODE_KEY -> "newcolumn").asJava)

    val allowTableSchemas = Seq(
      new StructType()
        .add("id", IntegerType.INTEGER)
        .add("name", StringType.STRING),
      new StructType()
        .add("id", IntegerType.INTEGER)
        .add("name", StringType.STRING)
        .add("someother", LongType.LONG),
      new StructType()
        .add("id", IntegerType.INTEGER)
        .add("name", StringType.STRING)
        .add("someother", new StructType().add("nestd", IntegerType.INTEGER)))
    allowTableSchemas.forall(tableSchema =>
      conf.getSchemaEvolutionPolicy.allowEvolve(tableSchema, schema))

    val blockTableSchemas = Seq(
      new StructType()
        .add("id", IntegerType.INTEGER),
      new StructType()
        .add("id", IntegerType.INTEGER)
        .add("name", IntegerType.INTEGER),
      new StructType()
        .add("id", IntegerType.INTEGER)
        .add("name", StringType.STRING, false))
    blockTableSchemas.forall(tableSchema =>
      !conf.getSchemaEvolutionPolicy.allowEvolve(tableSchema, schema))
  }

  test("schema evolution mode allow with physical name") {
    val schema = new StructType()
      .add("id", IntegerType.INTEGER)
      .add("name", StringType.STRING)
    val conf = new DeltaSinkConf(
      schema,
      Map(DeltaSinkConf.SCHEMA_EVOLUTION_MODE_KEY -> "newcolumn").asJava)

    val allowTableSchemas = Seq(
      new StructType()
        .add("id", IntegerType.INTEGER)
        .add(
          "name",
          StringType.STRING,
          true,
          FieldMetadata.builder()
            .putString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, "uuid1")
            .putString(ColumnMapping.COLUMN_MAPPING_ID_KEY, "1")
            .build()),
      new StructType()
        .add(
          "id",
          IntegerType.INTEGER,
          true,
          FieldMetadata.builder()
            .putString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, "uuid1")
            .putString(ColumnMapping.COLUMN_MAPPING_ID_KEY, "1")
            .build())
        .add("name", StringType.STRING)
        .add("someother", LongType.LONG),
      new StructType()
        .add("id", IntegerType.INTEGER)
        .add("name", StringType.STRING)
        .add(
          "someother",
          new StructType().add("nestd", IntegerType.INTEGER),
          true,
          FieldMetadata.builder()
            .putString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, "uuid1")
            .putString(ColumnMapping.COLUMN_MAPPING_ID_KEY, "1")
            .build()))
    allowTableSchemas.forall(tableSchema =>
      conf.getSchemaEvolutionPolicy.allowEvolve(tableSchema, schema))
  }
}
