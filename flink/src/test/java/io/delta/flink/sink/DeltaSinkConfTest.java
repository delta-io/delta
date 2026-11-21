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
import static org.junit.jupiter.api.Assertions.assertFalse;
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
            new StructType().add("name", StringType.STRING).add("id", IntegerType.INTEGER),
            new StructType()
                .add("id", IntegerType.INTEGER)
                .add("inserted", StringType.STRING)
                .add("name", StringType.STRING),
            new StructType().add("id", IntegerType.INTEGER).add("name", IntegerType.INTEGER),
            new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING, false),
            new StructType()
                .add("id", IntegerType.INTEGER)
                .add("name", StringType.STRING)
                .add("required", LongType.LONG, false),
            new StructType()
                .add("id", IntegerType.INTEGER)
                .add(
                    "name",
                    new StructType()
                        .add("last", StringType.STRING)
                        .add("first", StringType.STRING)));
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
                    "id",
                    IntegerType.INTEGER,
                    true,
                    FieldMetadata.builder()
                        .putString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, "uuid1")
                        .putString(ColumnMapping.COLUMN_MAPPING_ID_KEY, "1")
                        .build())
                .add("name", StringType.STRING),
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

    StructType reorderedTableSchema =
        new StructType()
            .add(
                "name",
                StringType.STRING,
                true,
                FieldMetadata.builder()
                    .putString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, "uuid1")
                    .putString(ColumnMapping.COLUMN_MAPPING_ID_KEY, "1")
                    .build())
            .add("id", IntegerType.INTEGER);
    assertFalse(conf.getSchemaEvolutionPolicy().allowEvolve(reorderedTableSchema, schema));
  }

  @Test
  void testNoEvolutionChecksLogicalNamesRecursively() {
    StructType schema =
        new StructType()
            .add("id", IntegerType.INTEGER)
            .add(
                "profile",
                new StructType().add("first", StringType.STRING).add("last", StringType.STRING));
    DeltaSinkConf conf = new DeltaSinkConf(schema, Map.of());

    assertTrue(conf.getSchemaEvolutionPolicy().allowEvolve(schema, schema));
    assertFalse(
        conf.getSchemaEvolutionPolicy()
            .allowEvolve(
                new StructType()
                    .add("renamed", IntegerType.INTEGER)
                    .add("profile", schema.at(1).getDataType()),
                schema));
    assertFalse(
        conf.getSchemaEvolutionPolicy()
            .allowEvolve(
                new StructType()
                    .add("id", IntegerType.INTEGER)
                    .add(
                        "profile",
                        new StructType()
                            .add("last", StringType.STRING)
                            .add("first", StringType.STRING)),
                schema));
  }

  @Test
  void testNoEvolutionChecksCollectionTypesRecursively() {
    StructType child =
        new StructType().add("first", StringType.STRING).add("last", StringType.STRING);
    StructType schema =
        new StructType()
            .add("items", new ArrayType(child, true))
            .add("lookup", new MapType(StringType.STRING, child, true));
    DeltaSinkConf conf = new DeltaSinkConf(schema, Map.of());
    StructType reorderedChild =
        new StructType().add("last", StringType.STRING).add("first", StringType.STRING);
    StructType mappedChild =
        new StructType()
            .add(
                "first",
                StringType.STRING,
                true,
                FieldMetadata.builder()
                    .putString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, "uuid1")
                    .build())
            .add("last", StringType.STRING);

    StructType mappedSchema =
        new StructType()
            .add("items", new ArrayType(mappedChild, true))
            .add("lookup", new MapType(StringType.STRING, mappedChild, true));
    assertTrue(conf.getSchemaEvolutionPolicy().allowEvolve(mappedSchema, schema));

    List<StructType> incompatibleSchemas =
        List.of(
            new StructType()
                .add("items", new ArrayType(reorderedChild, true))
                .add("lookup", new MapType(StringType.STRING, child, true)),
            new StructType()
                .add("items", new ArrayType(child, false))
                .add("lookup", new MapType(StringType.STRING, child, true)),
            new StructType()
                .add("items", new ArrayType(child, true))
                .add("lookup", new MapType(StringType.STRING, reorderedChild, true)),
            new StructType()
                .add("items", new ArrayType(child, true))
                .add("lookup", new MapType(StringType.STRING, child, false)));
    assertTrue(
        incompatibleSchemas.stream()
            .noneMatch(
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
