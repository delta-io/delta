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

package io.delta.flink.inttest;

import static io.delta.flink.sink.ConversionsTest.f;
import static io.delta.flink.sink.ConversionsTest.row;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.delta.flink.sink.DeltaSink;
import io.delta.flink.sink.DeltaSinkTest;
import io.delta.flink.sink.sql.SerializableFunction;
import java.net.URI;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;

public class DeltaSinkIntTest extends IntTestBase {

  static final String TEST_TABLE = "main.hao.flinkint_sink";

  public DeltaSinkIntTest(SparkSession spark, URI catalogEndpoint, String catalogToken) {
    super(spark, catalogEndpoint, catalogToken);
  }

  @BeforeEach
  public void prepare() {
    spark.sql(String.format("DROP TABLE IF EXISTS %s", TEST_TABLE));
  }

  @IntTest
  public void testWriteToManagedTable() {
    RowType nested = row(f("x", new IntType()), f("y", new VarCharType(32)));
    MapType map = new MapType(new VarCharType(VarCharType.MAX_LENGTH), new BigIntType());
    RowType flinkSchema =
        row(
            f("b", new BooleanType()),
            f("ti", new TinyIntType()),
            f("sm", new SmallIntType()),
            f("i", new IntType()),
            f("l", new BigIntType()),
            f("f", new FloatType()),
            f("d", new DoubleType()),
            f("dec", new DecimalType(18, 6)),
            f("s", new VarCharType(VarCharType.MAX_LENGTH)),
            f("bin", new VarBinaryType(VarBinaryType.MAX_LENGTH)),
            f("date", new DateType()),
            f("ts", new LocalZonedTimestampType(3)),
            f("array", new ArrayType(new IntType())),
            f("nest", nested),
            f("map", map));

    DeltaSink deltaSink =
        DeltaSink.builder()
            .withFlinkSchema(flinkSchema)
            .withConfigurations(
                Map.of(
                    "type",
                    "unitycatalog",
                    "unitycatalog.name",
                    "main",
                    "unitycatalog.table_name",
                    TEST_TABLE,
                    "unitycatalog.endpoint",
                    catalogEndpoint.toString(),
                    "unitycatalog.token",
                    catalogToken))
            .build();

    SerializableFunction<Integer, String> supplier = String::valueOf;
    SerializableFunction<String, RowData> parser =
        value -> {
          int idx = Integer.parseInt(value);
          GenericRowData row = new GenericRowData(15);

          row.setField(0, idx % 10 == 0); // BOOLEAN
          row.setField(1, (byte) (idx % 8)); // TINYINT
          row.setField(2, (short) idx); // SMALLINT
          row.setField(3, idx); // INT
          row.setField(4, Long.MAX_VALUE - idx); // BIGINT
          row.setField(5, 1.5f * idx); // FLOAT
          row.setField(6, 3.14159d * idx); // DOUBLE
          // DECIMAL(18,6)
          var decimal = DecimalData.fromBigDecimal(new java.math.BigDecimal("12345.678900"), 18, 6);
          row.setField(7, decimal);
          // STRING
          row.setField(8, StringData.fromString("string" + idx));
          // BINARY
          row.setField(9, new byte[] {1, 2, 3, 4, 5, 6, 7, 8, (byte) (idx % 10)});
          // DATE → days since epoch
          int days = (int) LocalDate.of(2026, 1, 19).toEpochDay();
          row.setField(10, days);
          // TIMESTAMP_LTZ(3) → epoch millis
          long epochMillis =
              LocalDateTime.of(2026, 1, 19, 12, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
          row.setField(11, TimestampData.fromEpochMillis(epochMillis));
          // ARRAY(INT)
          ArrayData arrayData = new GenericArrayData(new int[] {1, 2, 3, idx});
          row.setField(12, arrayData);
          // STRUCT(INT, STRING)
          RowData nestedData = GenericRowData.of(idx, StringData.fromString("nested" + idx));
          row.setField(13, nestedData);
          // MAP(STRING, LONG)
          MapData mapData =
              new GenericMapData(
                  Map.of(
                      StringData.fromString("map_1"),
                      Long.valueOf(idx % 3),
                      StringData.fromString("map_2"),
                      Long.valueOf(idx % 5)));
          row.setField(14, mapData);
          return row;
        };

    int rounds = 51;
    try {
      DeltaSinkTest.runSink(deltaSink, flinkSchema, rounds, supplier, parser);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    assertCount(51, spark.sql(String.format("SELECT COUNT(1) FROM %s", TEST_TABLE)));
    assertContent(
        spark.sql(String.format("SELECT * FROM %s ORDER BY i", TEST_TABLE)),
        row -> {
          int id = row.getInt(3);
          assertEquals(id % 10 == 0, row.getBoolean(0));
          assertEquals((byte) (id % 8), row.getByte(1));
          assertEquals((short) id, row.getShort(2));
          assertEquals(Long.MAX_VALUE - id, row.getLong(4));
          assertEquals(1.5f * id, row.getFloat(5), 0.01f);
          assertEquals(3.14159d * id, row.getDouble(6), 0.01d);
          assertEquals(new java.math.BigDecimal("12345.678900"), row.getDecimal(7));
          assertEquals("string" + id, row.getString(8));
          assertEquals(20472, row.getDate(10).toLocalDate().toEpochDay());
          assertEquals(1768824000000L, row.getTimestamp(11).toInstant().toEpochMilli());
          assertEquals(List.of(1, 2, 3, id), row.getList(12));
          assertEquals(id, row.getStruct(13).getInt(0));
          assertEquals("nested" + id, row.getStruct(13).getString(1));
          assertEquals((long) (id % 3), row.<String, Long>getMap(14).get("map_1").get());
          assertEquals((long) (id % 5), row.<String, Long>getMap(14).get("map_2").get());
        });
  }
}
