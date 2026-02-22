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

package io.delta.flink.sink;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.types.*;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** JUnit 6 test suite for {@link DeltaSinkConf}. */
class DeltaSinkConfTest {

  @Test
  void testSchemaEvolutionModeAllowWithoutPhysicalName() {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);
    DeltaSinkConf conf =
        new DeltaSinkConf(schema, Map.of(DeltaSinkConf.SCHEMA_EVOLUTION_MODE.key(), "newcolumn"));

    List<StructType> allowTableSchemas =
        List.of(
            new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING),
            new StructType()
                .add("id", IntegerType.INTEGER)
                .add("name", StringType.STRING)
                .add("someother", LongType.LONG),
            new StructType()
                .add("id", IntegerType.INTEGER)
                .add("name", StringType.STRING)
                .add("someother", new StructType().add("nestd", IntegerType.INTEGER)));
    assertTrue(
        allowTableSchemas.stream()
            .allMatch(
                tableSchema -> conf.getSchemaEvolutionPolicy().allowEvolve(tableSchema, schema)));

    List<StructType> blockTableSchemas =
        List.of(
            new StructType().add("id", IntegerType.INTEGER),
            new StructType().add("id", IntegerType.INTEGER).add("name", IntegerType.INTEGER),
            new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING, false));
    assertTrue(
        blockTableSchemas.stream()
            .allMatch(
                tableSchema -> !conf.getSchemaEvolutionPolicy().allowEvolve(tableSchema, schema)));
  }

  @Test
  void testSchemaEvolutionModeAllowWithPhysicalName() {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);
    DeltaSinkConf conf =
        new DeltaSinkConf(schema, Map.of(DeltaSinkConf.SCHEMA_EVOLUTION_MODE.key(), "newcolumn"));

    List<StructType> allowTableSchemas =
        List.of(
            new StructType()
                .add(
                    "name",
                    StringType.STRING,
                    true,
                    FieldMetadata.builder()
                        .putString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, "uuid1")
                        .putString(ColumnMapping.COLUMN_MAPPING_ID_KEY, "1")
                        .build())
                .add("id", IntegerType.INTEGER),
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
                        .build()));
    assertTrue(
        allowTableSchemas.stream()
            .allMatch(
                tableSchema -> conf.getSchemaEvolutionPolicy().allowEvolve(tableSchema, schema)));
  }
}
