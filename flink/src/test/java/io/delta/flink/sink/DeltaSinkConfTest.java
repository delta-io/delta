/*
 *  Copyright (2026) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.sink;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.types.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** JUnit test suite for {@link DeltaSinkConf}. */
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

  // ----------------------------------------------------------------------
  // write.mode / primary_key
  // ----------------------------------------------------------------------

  @Test
  void testWriteModeDefaultsToAppend() {
    StructType schema = new StructType().add("id", IntegerType.INTEGER);
    DeltaSinkConf conf = new DeltaSinkConf(schema, Map.of());

    assertEquals(DeltaSinkConf.WriteMode.APPEND, conf.getWriteMode());
    assertEquals(0, conf.getPrimaryKeyOrdinals().length);
    assertTrue(!conf.isUpsert());
  }

  @Test
  void testUpsertModeRequiresPrimaryKey() {
    StructType schema = new StructType().add("id", IntegerType.INTEGER);
    Map<String, String> opts = new HashMap<>();
    opts.put(DeltaSinkConf.WRITE_MODE.key(), "upsert");

    assertThrows(IllegalArgumentException.class, () -> new DeltaSinkConf(schema, opts));
  }

  @Test
  void testUpsertModeWithPrimaryKey() {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);
    Map<String, String> opts = new HashMap<>();
    opts.put(DeltaSinkConf.WRITE_MODE.key(), "upsert");
    opts.put(DeltaSinkConf.PRIMARY_KEY.key(), "0");

    DeltaSinkConf conf = new DeltaSinkConf(schema, opts);
    assertEquals(DeltaSinkConf.WriteMode.UPSERT, conf.getWriteMode());
    assertTrue(conf.isUpsert());
    assertArrayEquals(new int[] {0}, conf.getPrimaryKeyOrdinals());
  }

  @Test
  void testPrimaryKeyOrdinalParsing() {
    StructType schema =
        new StructType()
            .add("a", IntegerType.INTEGER)
            .add("b", IntegerType.INTEGER)
            .add("c", IntegerType.INTEGER);
    Map<String, String> opts = new HashMap<>();
    opts.put(DeltaSinkConf.WRITE_MODE.key(), "upsert");
    opts.put(DeltaSinkConf.PRIMARY_KEY.key(), " 0 ,1,  2  ");

    DeltaSinkConf conf = new DeltaSinkConf(schema, opts);
    assertArrayEquals(new int[] {0, 1, 2}, conf.getPrimaryKeyOrdinals());
  }

  @Test
  void testNonIntegerPrimaryKeyThrows() {
    // Stale name-based wire-format value should now fail loudly instead of being silently
    // misinterpreted; this guards against accidental regressions of the contract.
    StructType schema = new StructType().add("id", IntegerType.INTEGER);
    Map<String, String> opts = new HashMap<>();
    opts.put(DeltaSinkConf.WRITE_MODE.key(), "upsert");
    opts.put(DeltaSinkConf.PRIMARY_KEY.key(), "id");

    assertThrows(IllegalArgumentException.class, () -> new DeltaSinkConf(schema, opts));
  }

  @Test
  void testOutOfRangePrimaryKeyOrdinalThrows() {
    StructType schema = new StructType().add("id", IntegerType.INTEGER);
    Map<String, String> opts = new HashMap<>();
    opts.put(DeltaSinkConf.WRITE_MODE.key(), "upsert");
    opts.put(DeltaSinkConf.PRIMARY_KEY.key(), "5");

    assertThrows(IllegalArgumentException.class, () -> new DeltaSinkConf(schema, opts));
  }

  @Test
  void testNegativePrimaryKeyOrdinalThrows() {
    StructType schema = new StructType().add("id", IntegerType.INTEGER);
    Map<String, String> opts = new HashMap<>();
    opts.put(DeltaSinkConf.WRITE_MODE.key(), "upsert");
    opts.put(DeltaSinkConf.PRIMARY_KEY.key(), "-1");

    assertThrows(IllegalArgumentException.class, () -> new DeltaSinkConf(schema, opts));
  }

  @Test
  void testWriteModeIsCaseInsensitive() {
    StructType schema = new StructType().add("id", IntegerType.INTEGER);
    Map<String, String> opts = new HashMap<>();
    opts.put(DeltaSinkConf.WRITE_MODE.key(), "UPSERT");
    opts.put(DeltaSinkConf.PRIMARY_KEY.key(), "0");

    DeltaSinkConf conf = new DeltaSinkConf(schema, opts);
    assertEquals(DeltaSinkConf.WriteMode.UPSERT, conf.getWriteMode());
  }

  @Test
  void testUnknownWriteModeThrows() {
    StructType schema = new StructType().add("id", IntegerType.INTEGER);
    Map<String, String> opts = new HashMap<>();
    opts.put(DeltaSinkConf.WRITE_MODE.key(), "merge");

    assertThrows(IllegalArgumentException.class, () -> new DeltaSinkConf(schema, opts));
  }
}
