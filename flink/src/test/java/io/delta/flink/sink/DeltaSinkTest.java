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

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.delta.flink.MockHttp;
import io.delta.flink.TestHelper;
import io.delta.flink.sink.sql.SerializableFunction;
import io.delta.flink.table.CatalogManagedTable;
import io.delta.flink.table.HadoopTable;
import io.delta.flink.table.UnityCatalog;
import io.delta.kernel.Snapshot.ChecksumWriteMode;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.types.*;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.utils.CloseableIterable;
import java.io.File;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.*;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/** JUnit 6 test suite for {@link DeltaSink}. */
class DeltaSinkTest extends TestHelper {

  private void runSink(
      DeltaSink sink,
      RowType flinkSchema,
      int rounds,
      SerializableFunction<Integer, String> supplier,
      SerializableFunction<String, RowData> parser)
      throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(5);
    env.enableCheckpointing(100);

    // Use String to make the StreamSource serializable
    List<String> dataList =
        IntStream.range(0, rounds).mapToObj(supplier::apply).collect(Collectors.toList());

    var dataType = DataTypes.of(flinkSchema);
    var rowType = (RowType) dataType.getLogicalType();
    TypeInformation<RowData> rowDataTypeInfo = InternalTypeInfo.of(rowType);

    DataStream<RowData> input =
        env.fromSource(
                new DelayFinishTestSource<>(dataList, 2),
                WatermarkStrategy.noWatermarks(),
                "source")
            .returns(String.class)
            .map(
                new MapFunction<String, RowData>() {
                  @Override
                  public RowData map(String value) throws Exception {
                    return parser.apply(value);
                  }
                })
            .returns(rowDataTypeInfo);
    input.sinkTo(sink).uid("deltaSink");
    env.execute("DeltaSink integration test");
  }

  @Test
  void testMiniE2eTestToEmptyTable() {
    withTempDir(
        dir -> {
          String tablePath = dir.getPath();
          RowType flinkSchema =
              RowType.of(
                  new LogicalType[] {
                    new BooleanType(),
                    new TinyIntType(),
                    new SmallIntType(),
                    new IntType(),
                    new BigIntType(),
                    new FloatType(),
                    new DoubleType(),
                    new DecimalType(18, 6),
                    new CharType(10),
                    new VarCharType(50),
                    new VarCharType(VarCharType.MAX_LENGTH),
                    new BinaryType(8),
                    new VarBinaryType(16),
                    new VarBinaryType(VarBinaryType.MAX_LENGTH),
                    new DateType(),
                    new TimeType(3),
                    new TimestampType(3),
                    new LocalZonedTimestampType(3),
                    new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.YEAR, 3),
                    new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY, 3, 6)
                  },
                  new String[] {
                    "f_boolean",
                    "f_tinyint",
                    "f_smallint",
                    "f_int",
                    "f_bigint",
                    "f_float",
                    "f_double",
                    "f_decimal",
                    "f_char",
                    "f_varchar",
                    "f_string",
                    "f_binary",
                    "f_varbinary",
                    "f_bytes",
                    "f_date",
                    "f_time",
                    "f_timestamp",
                    "f_timestamp_ltz",
                    "f_interval_ym",
                    "f_interval_dt"
                  });
          StructType deltaSchema = Conversions.FlinkToDelta.schema(flinkSchema);

          DeltaSink deltaSink =
              DeltaSink.builder()
                  .withTablePath(tablePath)
                  .withFlinkSchema(flinkSchema)
                  .withPartitionColNames(Collections.emptyList())
                  .build();

          SerializableFunction<Integer, String> supplier = String::valueOf;
          SerializableFunction<String, RowData> parser =
              value -> {
                int idx = Integer.parseInt(value);
                GenericRowData row = new GenericRowData(20);

                row.setField(0, idx % 10 == 0); // BOOLEAN
                row.setField(1, (byte) (idx % 8)); // TINYINT
                row.setField(2, (short) idx); // SMALLINT
                row.setField(3, idx); // INT
                row.setField(4, Long.MAX_VALUE - idx); // BIGINT
                row.setField(5, 1.5f * idx); // FLOAT
                row.setField(6, 3.14159d * idx); // DOUBLE
                // DECIMAL(18,6)
                var decimal =
                    DecimalData.fromBigDecimal(new java.math.BigDecimal("12345.678900"), 18, 6);
                row.setField(7, decimal);
                // CHAR / VARCHAR / STRING
                row.setField(8, StringData.fromString("char" + idx));
                row.setField(9, StringData.fromString("varchar" + idx));
                row.setField(10, StringData.fromString("string" + idx));
                // BINARY / VARBINARY / BYTES
                row.setField(11, new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
                row.setField(12, new byte[] {9, 10, 11});
                row.setField(13, new byte[] {42});
                // DATE → days since epoch
                int days = (int) LocalDate.of(2026, 1, 19).toEpochDay();
                row.setField(14, days);
                // TIME(3) → millis of day
                int millisOfDay = 12 * 60 * 60 * 1000;
                row.setField(15, millisOfDay);
                // TIMESTAMP(3)
                var ts = TimestampData.fromLocalDateTime(LocalDateTime.of(2026, 1, 19, 12, 0));
                row.setField(16, ts);
                // TIMESTAMP_LTZ(3) → epoch millis
                long epochMillis =
                    LocalDateTime.of(2026, 1, 19, 12, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
                row.setField(17, TimestampData.fromEpochMillis(epochMillis));
                // INTERVAL YEAR TO MONTH → total months
                int months = 2 * 12 + 3; // 2 years 3 months
                row.setField(18, months);
                // INTERVAL DAY TO SECOND(3) → milliseconds
                long millis = 3L * 24 * 60 * 60 * 1000; // 3 days
                row.setField(19, millis);

                return row;
              };

          int rounds = 100000;
          try {
            runSink(deltaSink, flinkSchema, rounds, supplier, parser);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }

          verifyTableContent(
              tablePath,
              (version, actions, props) -> {
                List<AddFile> actionList = new ArrayList<>();
                actions.iterator().forEachRemaining(actionList::add);
                assertEquals(
                    rounds, actionList.stream().mapToLong(a -> a.getNumRecords().get()).sum());
                actionList.forEach(
                    addfile -> {
                      List<Row> rows =
                          readParquet(
                              new File(tablePath).toPath().resolve(addfile.getPath()), deltaSchema);
                      rows.forEach(
                          row -> {
                            int id = row.getInt(3);
                            assertEquals(id % 10 == 0, row.getBoolean(0));
                            assertEquals((byte) (id % 8), row.getByte(1));
                            assertEquals((short) id, row.getShort(2));
                            assertEquals(Long.MAX_VALUE - id, row.getLong(4));
                            assertEquals("char" + id, row.getString(8));
                            assertEquals("varchar" + id, row.getString(9));
                            assertEquals("string" + id, row.getString(10));
                            assertEquals(20472, row.getInt(14));
                            assertEquals(43200000, row.getInt(15));
                            assertEquals(1768824000000000L, row.getLong(16));
                            assertEquals(1768824000000000L, row.getLong(17));
                            assertEquals(27, row.getInt(18));
                            assertEquals(259200000L, row.getLong(19));
                          });
                    });
              });
        });
  }

  @Test
  void testMinE2eTestToExistingTable() {
    withTempDir(
        dir -> {
          String tablePath = dir.getPath();
          DefaultEngine defaultEngine = DefaultEngine.create(new Configuration());
          StructType deltaSchema =
              new StructType()
                  .add("id", IntegerType.INTEGER)
                  .add(
                      "content",
                      new StructType()
                          .add("nested_name", StringType.STRING)
                          .add("nested_list", new ArrayType(StringType.STRING, true)))
                  .add("part", StringType.STRING);
          // Create non-empty table
          var createdTable =
              TableManager.buildCreateTableTransaction(tablePath, deltaSchema, "dummyEngine")
                  .build(defaultEngine)
                  .commit(defaultEngine, CloseableIterable.emptyIterable());
          createdTable
              .getPostCommitSnapshot()
              .ifPresent(
                  wrap(
                      snapshot -> snapshot.writeChecksum(defaultEngine, ChecksumWriteMode.SIMPLE)));

          RowType flinkSchema =
              RowType.of(
                  new LogicalType[] {
                    new IntType(),
                    RowType.of(
                        new LogicalType[] {
                          new VarCharType(VarCharType.MAX_LENGTH),
                          new org.apache.flink.table.types.logical.ArrayType(
                              new VarCharType(VarCharType.MAX_LENGTH))
                        },
                        new String[] {"nested_name", "nested_list"}),
                    new VarCharType(VarCharType.MAX_LENGTH)
                  },
                  new String[] {"id", "content", "part"});

          DeltaSink deltaSink =
              DeltaSink.builder()
                  .withTablePath(tablePath)
                  .withFlinkSchema(flinkSchema)
                  .withPartitionColNames(Collections.emptyList())
                  .build();
          // Use String to make the StreamSource serializable
          Random random = new Random(System.currentTimeMillis());
          SerializableFunction<Integer, String> supplier =
              idx -> {
                int listSize = 5 + random.nextInt(10);
                String listItems =
                    IntStream.range(0, listSize)
                        .mapToObj(i -> "\"d" + i + "\"")
                        .collect(Collectors.joining(","));
                return String.format(
                    "{\n"
                        + "  \"id\": %d,\n"
                        + "  \"content\": {\n"
                        + "    \"nested_name\": \"n%d\",\n"
                        + "    \"nested_list\": [%s]\n"
                        + "  },\n"
                        + "  \"part\": \"p%d\"\n"
                        + "}",
                    idx, idx, listItems, idx % 10);
              };
          SerializableFunction<String, RowData> parser =
              value -> {
                try {
                  var tree = new ObjectMapper().readTree(value);
                  int id = tree.get("id").asInt();
                  var part = StringData.fromString(tree.get("part").asText());
                  var nested = tree.get("content");
                  var nname = StringData.fromString(nested.get("nested_name").asText());
                  var narray = (ArrayNode) nested.get("nested_list");
                  Object[] list = new Object[narray.size()];
                  for (int i = 0; i < narray.size(); i++) {
                    list[i] = StringData.fromString(narray.get(i).asText());
                  }
                  return GenericRowData.of(
                      id, GenericRowData.of(nname, new GenericArrayData(list)), part);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              };

          int rounds = 100000;
          try {
            runSink(deltaSink, flinkSchema, rounds, supplier, parser);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }

          // Read the table to make sure the data is correct.
          verifyTableContent(
              tablePath,
              (version, actions, props) -> {
                List<AddFile> actionList = new ArrayList<>();
                actions.iterator().forEachRemaining(actionList::add);
                assertEquals(
                    rounds, actionList.stream().mapToLong(a -> a.getNumRecords().get()).sum());
                actionList.forEach(
                    addfile -> {
                      List<Row> rows =
                          readParquet(
                              new File(tablePath).toPath().resolve(addfile.getPath()), deltaSchema);
                      rows.forEach(
                          row -> {
                            int id = row.getInt(0);
                            assertEquals("p" + (id % 10), row.getString(2));
                            var content = row.getStruct(1);
                            assertEquals("n" + id, content.getString(0));
                            var array = content.getArray(1);
                            assertTrue(array.getSize() <= 15);
                            for (int i = 0; i < array.getSize(); i++) {
                              assertEquals("d" + i, array.getElements().getString(i));
                            }
                          });
                    });
              });
        });
  }

  @Test
  void testCreateWriterAndCommitter() {
    withTempDir(
        dir -> {
          String tablePath = dir.getPath();
          RowType flinkSchema =
              RowType.of(
                  new LogicalType[] {new IntType(), new VarCharType(VarCharType.MAX_LENGTH)},
                  new String[] {"id", "part"});

          DeltaSink deltaSink =
              DeltaSink.builder()
                  .withTablePath(tablePath)
                  .withFlinkSchema(flinkSchema)
                  .withPartitionColNames(List.of("part"))
                  .build();

          assertNotNull(deltaSink.createWriter(new TestWriterInitContext(1, 1, 1)));
          assertNotNull(deltaSink.createCommitter(new TestCommitterInitContext(1, 1, 1)));
        });
  }

  @Test
  void testSinkIsSerializable() {
    withTempDir(
        dir -> {
          String tablePath = dir.getPath();
          RowType flinkSchema =
              RowType.of(
                  new LogicalType[] {new IntType(), new VarCharType(VarCharType.MAX_LENGTH)},
                  new String[] {"id", "part"});

          DeltaSink deltaSink =
              DeltaSink.builder()
                  .withTablePath(tablePath)
                  .withFlinkSchema(flinkSchema)
                  .withPartitionColNames(List.of("part"))
                  .build();
          try {
            byte[] serialized = InstantiationUtil.serializeObject(deltaSink);
            DeltaSink copy =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
            assertNotNull(copy);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  void testSinkBuilder() {
    RowType flinkSchema =
        RowType.of(
            new LogicalType[] {new IntType(), new VarCharType(VarCharType.MAX_LENGTH)},
            new String[] {"id", "part"});
    withTempDir(
        dir -> {
          DeltaSink sink1 =
              DeltaSink.builder()
                  .withFlinkSchema(flinkSchema)
                  .withConfigurations(
                      Map.of("type", "hadoop", "hadoop.table_path", dir.getAbsolutePath()))
                  .build();
          assertTrue(sink1.getTable() instanceof HadoopTable);
          assertEquals(
              String.format("file://%s/", dir.getAbsolutePath()),
              ((HadoopTable) sink1.getTable()).getTablePath().toString());
        });
    withTempDir(
        dir ->
            MockHttp.withMock(
                MockHttp.forNewUCTable("123", dir.getAbsolutePath()),
                mockHttp -> {
                  DeltaSink sink2 =
                      DeltaSink.builder()
                          .withFlinkSchema(flinkSchema)
                          .withConfigurations(
                              Map.of(
                                  "type",
                                  "unitycatalog",
                                  "unitycatalog.name",
                                  "ab",
                                  "unitycatalog.table_name",
                                  "ab.cd.ef",
                                  "unitycatalog.endpoint",
                                  mockHttp.uri().toString(),
                                  "unitycatalog.token",
                                  "wow"))
                          .build();
                  assertTrue(sink2.getTable() instanceof CatalogManagedTable);
                  CatalogManagedTable table = (CatalogManagedTable) sink2.getTable();
                  assertTrue(table.getCatalog() instanceof UnityCatalog);
                  assertEquals("ab.cd.ef", table.getId());
                  assertEquals("ab", ((UnityCatalog) table.getCatalog()).getName());
                }));
  }
}
