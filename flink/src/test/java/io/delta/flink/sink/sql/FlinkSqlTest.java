/*
 *  Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.flink.sink.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.delta.flink.TestHelper;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.types.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** JUnit 6 test suite for Flink SQL integration with Delta. */
class FlinkSqlTest extends TestHelper {

  @Test
  void testLoadIntoHadoopTable() {
    withTempDir(
        dir -> {
          EnvironmentSettings settings =
              EnvironmentSettings.newInstance().inStreamingMode().build();
          TableEnvironment tEnv = TableEnvironment.create(settings);
          int numRecords = 5000;
          tEnv.executeSql(
              "CREATE TEMPORARY TABLE src (\n"
                  + "id BIGINT,\n"
                  + "dt STRING\n"
                  + ") WITH (\n"
                  + "'connector' = 'datagen',\n"
                  + "'rows-per-second' = '1000',\n"
                  + "'fields.id.kind' = 'sequence',\n"
                  + "'fields.id.start' = '1',\n"
                  + "'fields.id.end' = '"
                  + numRecords
                  + "'\n"
                  + ")");

          tEnv.executeSql(
              "CREATE TEMPORARY TABLE sink (\n"
                  + "  id BIGINT,\n"
                  + "  dt STRING\n"
                  + ") WITH (\n"
                  + "  'connector' = 'delta',\n"
                  + "  'table_path' = '"
                  + dir.getPath()
                  + "',\n"
                  + "  'sink.parallelism' = '4',\n"
                  + "  'uid' = 'someuid'\n"
                  + ")");

          tEnv.executeSql("INSERT INTO sink SELECT id, dt FROM src").await();

          StructType schema =
              new StructType().add("id", LongType.LONG).add("dt", StringType.STRING);
          // Check the table content
          verifyTableContent(
              dir.getPath(),
              (version, addfiles, properties) -> {
                List<AddFile> actionList = new ArrayList<>();
                addfiles.iterator().forEachRemaining(actionList::add);
                assertEquals(
                    numRecords, actionList.stream().mapToLong(a -> a.getNumRecords().get()).sum());
                Set<Long> records =
                    actionList.stream()
                        .flatMap(
                            addfile ->
                                readParquet(dir.toPath().resolve(addfile.getPath()), schema)
                                    .stream()
                                    .map(row -> row.getLong(0)))
                        .collect(Collectors.toSet());
                Set<Long> expected =
                    IntStream.rangeClosed(1, numRecords)
                        .mapToObj(Long::valueOf)
                        .collect(Collectors.toSet());
                assertEquals(expected, records);
              });
        });
  }

  @Test
  void testLoadIntoPartitionedHadoopTable() {
    withTempDir(
        dir -> {
          EnvironmentSettings settings =
              EnvironmentSettings.newInstance().inStreamingMode().build();
          TableEnvironment tEnv = TableEnvironment.create(settings);
          int numRecords = 1000;
          tEnv.executeSql(
              "CREATE TEMPORARY TABLE src (\n"
                  + "id BIGINT,\n"
                  + "dt STRING\n"
                  + ") WITH (\n"
                  + "'connector' = 'datagen',\n"
                  + "'number-of-rows' = '"
                  + numRecords
                  + "',\n"
                  + "'rows-per-second' = '100',\n"
                  + "'fields.id.kind' = 'sequence',\n"
                  + "'fields.id.start' = '1',\n"
                  + "'fields.id.end' = '"
                  + numRecords
                  + "',\n"
                  + "'fields.dt.kind' = 'random',\n"
                  + "'fields.dt.length' = '1'\n"
                  + ")");

          tEnv.executeSql(
              "CREATE TEMPORARY TABLE sink (\n"
                  + "  id BIGINT,\n"
                  + "  dt STRING\n"
                  + ") WITH (\n"
                  + "  'connector' = 'delta',\n"
                  + "  'table_path' = '"
                  + dir.getPath()
                  + "',\n"
                  + "  'partitions' = 'dt',\n"
                  + "  'uid' = 'someuid'\n"
                  + ")");

          tEnv.executeSql("INSERT INTO sink SELECT id, dt FROM src").await();

          StructType schema =
              new StructType().add("id", LongType.LONG).add("dt", StringType.STRING);
          // Check the table content
          verifyTableContent(
              dir.getPath(),
              (version, addfiles, properties) -> {
                List<AddFile> actionList = new ArrayList<>();
                addfiles.iterator().forEachRemaining(actionList::add);
                assertEquals(
                    numRecords, actionList.stream().mapToLong(a -> a.getNumRecords().get()).sum());
                Set<Long> records =
                    actionList.stream()
                        .flatMap(
                            addfile ->
                                readParquet(dir.toPath().resolve(addfile.getPath()), schema)
                                    .stream()
                                    .map(row -> row.getLong(0)))
                        .collect(Collectors.toSet());
                Set<Long> expected =
                    IntStream.rangeClosed(1, numRecords)
                        .mapToObj(Long::valueOf)
                        .collect(Collectors.toSet());
                assertEquals(expected, records);
                assertEquals(
                    16,
                    actionList.stream()
                        .map(a -> a.getPartitionValues().getValues().getString(0))
                        .collect(Collectors.toSet())
                        .size());
              });
        });
  }

  @Test
  void testLoadIntoTableWithManyTypes() {
    withTempDir(
        dir -> {
          EnvironmentSettings settings =
              EnvironmentSettings.newInstance().inStreamingMode().build();
          TableEnvironment tEnv = TableEnvironment.create(settings);
          int numRecords = 1000;
          tEnv.executeSql(
              "CREATE TABLE src (\n"
                  + "   c_tinyint    TINYINT,\n"
                  + "   c_smallint   SMALLINT,\n"
                  + "   c_int        INT,\n"
                  + "   c_bigint     BIGINT,\n"
                  + "   c_decimal    DECIMAL(18, 6),\n"
                  + "   c_float      FLOAT,\n"
                  + "   c_double     DOUBLE,\n"
                  + "   c_bool       BOOLEAN,\n"
                  + "   c_char       CHAR(8),\n"
                  + "   c_varchar    VARCHAR(32),\n"
                  + "   c_string     STRING,\n"
                  + "   c_binary     BINARY(8),\n"
                  + "   c_varbinary  VARBINARY(16),\n"
                  + "   c_bytes      BYTES,\n"
                  + "   c_date       DATE,\n"
                  + "   c_time       TIME(0)\n"
                  + ") WITH (\n"
                  + "   'connector' = 'datagen',\n"
                  + "   'number-of-rows' = '"
                  + numRecords
                  + "',\n"
                  + "   'rows-per-second' = '500',\n"
                  + "   'fields.c_tinyint.min' = '0',\n"
                  + "   'fields.c_tinyint.max' = '100',\n"
                  + "   'fields.c_smallint.min' = '0',\n"
                  + "   'fields.c_smallint.max' = '1000',\n"
                  + "   'fields.c_int.min' = '0',\n"
                  + "   'fields.c_int.max' = '100000',\n"
                  + "   'fields.c_bigint.min' = '0',\n"
                  + "   'fields.c_bigint.max' = '1000000000',\n"
                  + "   'fields.c_decimal.min' = '0',\n"
                  + "   'fields.c_decimal.max' = '999999',\n"
                  + "   'fields.c_float.min' = '0',\n"
                  + "   'fields.c_float.max' = '100',\n"
                  + "   'fields.c_double.min' = '0',\n"
                  + "   'fields.c_double.max' = '1000',\n"
                  + "   'fields.c_bool.kind' = 'random',\n"
                  + "   'fields.c_varchar.length' = '16',\n"
                  + "   'fields.c_string.length' = '24',\n"
                  + "   'fields.c_varbinary.length' = '16',\n"
                  + "   'fields.c_bytes.length' = '12'\n"
                  + ")");

          tEnv.executeSql(
              "CREATE TEMPORARY TABLE sink (\n"
                  + "  c_tinyint    TINYINT,\n"
                  + "  c_smallint   SMALLINT,\n"
                  + "  c_int        INT,\n"
                  + "    c_bigint     BIGINT,\n"
                  + "    c_decimal    DECIMAL(18, 6),\n"
                  + "    c_float      FLOAT,\n"
                  + "    c_double     DOUBLE,\n"
                  + "    c_bool       BOOLEAN,\n"
                  + "    c_char       CHAR(8),\n"
                  + "    c_varchar    VARCHAR(32),\n"
                  + "    c_string     STRING,\n"
                  + "    c_binary     BINARY(8),\n"
                  + "    c_varbinary  VARBINARY(16),\n"
                  + "    c_bytes      BYTES,\n"
                  + "    c_date       DATE,\n"
                  + "    c_time       TIME(0)\n"
                  + ") WITH (\n"
                  + "  'connector' = 'delta',\n"
                  + "  'table_path' = '"
                  + dir.getPath()
                  + "',\n"
                  + "  'uid' = 'someuid'\n"
                  + ")");

          tEnv.executeSql(
                  "INSERT INTO sink SELECT\n"
                      + " c_tinyint,\n"
                      + " c_smallint,\n"
                      + " c_int,\n"
                      + " c_bigint,\n"
                      + " c_decimal,\n"
                      + " c_float,\n"
                      + " c_double,\n"
                      + " c_bool,\n"
                      + " c_char,\n"
                      + " c_varchar,\n"
                      + " c_string,\n"
                      + " c_binary,\n"
                      + " c_varbinary,\n"
                      + " c_bytes,\n"
                      + " c_date,\n"
                      + " c_time\n"
                      + " FROM src")
              .await();
        });
  }

  @Test
  @Disabled("load uc table")
  void testLoadUcTable() throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    TableEnvironment tEnv = TableEnvironment.create(settings);
    int numRecords = 5000;
    tEnv.executeSql(
        "CREATE TEMPORARY TABLE src (\n"
            + "id INT,\n"
            + "name STRING\n"
            + ") WITH (\n"
            + "'connector' = 'datagen',\n"
            + "'rows-per-second' = '1000',\n"
            + "'fields.id.kind' = 'sequence',\n"
            + "'fields.id.start' = '1',\n"
            + "'fields.id.end' = '"
            + numRecords
            + "'\n"
            + ")");

    tEnv.executeSql(
        "CREATE CATALOG main WITH (\n"
            + "   'type' = 'unitycatalog',\n"
            + "   'endpoint' = 'https://e2-dogfood.staging.cloud.databricks.com/',\n"
            + "   'token' = '<REMOVED>'\n"
            + ")");

    tEnv.executeSql("INSERT INTO main.hao.writetest SELECT id, name FROM src").await();
  }
}
