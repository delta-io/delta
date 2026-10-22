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

package io.delta.flink.sink.dynamic;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.flink.TestHelper;
import io.delta.flink.sink.Conversions;
import io.delta.flink.sink.DelayFinishTestSource;
import io.delta.flink.sink.TestCommitterInitContext;
import io.delta.flink.sink.TestWriterInitContext;
import io.delta.flink.sink.sql.SerializableFunction;
import io.delta.flink.table.DeltaTable;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.io.File;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.sink2.InitContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

/** JUnit tests for {@link DeltaDynamicSink}. */
class DeltaDynamicSinkTest extends TestHelper {

  private static final StructType SCHEMA =
      new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

  /**
   * Same pipeline shape as {@link io.delta.flink.sink.DeltaSinkTest#runSink} but for {@link
   * DeltaDynamicSink} and {@link DynamicRow}.
   */
  static void runDynamicSink(
      DeltaDynamicSink sink,
      int rounds,
      SerializableFunction<Integer, String> supplier,
      SerializableFunction<String, DynamicRow> parser)
      throws Exception {
    Configuration flinkConf = new Configuration();
    flinkConf.set(SecurityOptions.DELEGATION_TOKENS_ENABLED, false);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConf);
    env.setParallelism(5);
    env.enableCheckpointing(100);

    List<String> dataList =
        IntStream.range(0, rounds).mapToObj(supplier::apply).collect(Collectors.toList());

    DataStream<DynamicRow> input =
        env.fromSource(
                new DelayFinishTestSource<>(dataList, 2),
                WatermarkStrategy.noWatermarks(),
                "source")
            .returns(String.class)
            .map(
                new MapFunction<String, DynamicRow>() {
                  @Override
                  public DynamicRow map(String value) {
                    return parser.apply(value);
                  }
                });
    input.sinkTo(sink).uid("deltaDynamicSink");
    env.execute("DeltaDynamicSink integration test");
  }

  /** Serializable {@link DynamicRow} for Flink routing integration tests. */
  static final class SerializableDynamicRow implements DynamicRow, java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private final String tableName;
    private final String schemaStr;
    private final String[] partitionColumns;
    private final String[] partitionValues;
    private final GenericRowData row;

    SerializableDynamicRow(
        String tableName,
        String schemaStr,
        String[] partitionColumns,
        String[] partitionValues,
        GenericRowData row) {
      this.tableName = tableName;
      this.schemaStr = schemaStr;
      this.partitionColumns = partitionColumns;
      this.partitionValues = partitionValues;
      this.row = row;
    }

    @Override
    public String getTableName() {
      return tableName;
    }

    @Override
    public String getSchemaStr() {
      return schemaStr;
    }

    @Override
    public RowData getRow() {
      return row;
    }

    @Override
    public String[] getPartitionColumns() {
      return partitionColumns;
    }

    @Override
    public String[] getPartitionValues() {
      return partitionValues;
    }
  }

  @Test
  void testBuilderRequiresTableProvider() {
    assertThrows(NullPointerException.class, () -> DeltaDynamicSink.builder().build());
  }

  @Test
  void testBuilderWithTableProviderAndConf() {
    ProviderFixture fx = new ProviderFixture(SCHEMA);
    DeltaDynamicSink sink =
        DeltaDynamicSink.builder()
            .withTableProvider(fx.provider())
            .withConf(Map.of("k", "v"))
            .build();
    assertNotNull(sink);
    assertNotNull(sink.createWriter(new TestWriterInitContext(0, 2, 0)));
    assertNotNull(sink.createCommitter(new TestCommitterInitContext(0, 2, 0)));
  }

  @Test
  void testGetWriteResultAndCommittableSerializers() {
    ProviderFixture fx = new ProviderFixture(SCHEMA);
    DeltaDynamicSink sink = DeltaDynamicSink.builder().withTableProvider(fx.provider()).build();
    assertNotNull(sink.getWriteResultSerializer());
    assertNotNull(sink.getCommittableSerializer());
    assertEquals(2, sink.getWriteResultSerializer().getVersion());
    assertEquals(2, sink.getCommittableSerializer().getVersion());
  }

  @Test
  void testDeprecatedCreateWriterThrows() {
    ProviderFixture fx = new ProviderFixture(SCHEMA);
    DeltaDynamicSink sink = DeltaDynamicSink.builder().withTableProvider(fx.provider()).build();
    assertThrows(UnsupportedOperationException.class, () -> sink.createWriter((InitContext) null));
  }

  @Test
  void testSerializableWithMinimalProvider() throws Exception {
    DeltaDynamicSink sink =
        DeltaDynamicSink.builder()
            .withTableProvider(new ThrowingSerializableProvider())
            .withConf(Map.of("x", "y"))
            .build();
    byte[] bytes = InstantiationUtil.serializeObject(sink);
    DeltaDynamicSink copy = InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
    assertNotNull(copy);
    assertNotNull(copy.getCommittableSerializer());
  }

  /**
   * End-to-end write to five on-disk Hadoop tables (100 records each) with the same wide schema and
   * row shape as {@link io.delta.flink.sink.DeltaSinkTest#testMiniE2eTestToEmptyTable}.
   */
  @Test
  void testMiniE2e() {
    final int numTables = 5;
    final int recordsPerTable = 100;
    final int rounds = numTables * recordsPerTable;

    withTempDir(
        baseDir -> {
          String[] tablePaths = new String[numTables];
          for (int i = 0; i < numTables; i++) {
            tablePaths[i] = new File(baseDir, "t" + i).getAbsolutePath();
          }

          RowType flinkSchema = wideMiniE2eFlinkSchema();
          StructType deltaSchema = Conversions.FlinkToDelta.schema(flinkSchema);
          String schemaJson = DataTypeJsonSerDe.serializeStructType(deltaSchema);

          DeltaDynamicSink sink =
              DeltaDynamicSink.builder()
                  .withTableProvider(new HadoopTableProvider(Collections.emptyMap()))
                  .withConf(Collections.emptyMap())
                  .build();

          SerializableFunction<Integer, String> supplier = String::valueOf;
          SerializableFunction<String, DynamicRow> parser =
              value -> {
                int idx = Integer.parseInt(value);
                int tableIdx = idx / recordsPerTable;
                return new SerializableDynamicRow(
                    tablePaths[tableIdx],
                    schemaJson,
                    new String[0],
                    new String[0],
                    wideMiniE2eGenericRow(idx));
              };

          try {
            runDynamicSink(sink, rounds, supplier, parser);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }

          for (int t = 0; t < numTables; t++) {
            final int tableIndex = t;
            String tablePath = tablePaths[t];
            verifyTableContent(
                tablePath,
                (version, actions, props) -> {
                  List<AddFile> actionList = new ArrayList<>();
                  actions.iterator().forEachRemaining(actionList::add);
                  assertEquals(
                      recordsPerTable,
                      actionList.stream().mapToLong(a -> a.getNumRecords().get()).sum());
                  actionList.forEach(
                      addfile -> {
                        List<Row> rows =
                            readParquet(
                                new File(tablePath).toPath().resolve(addfile.getPath()),
                                deltaSchema);
                        rows.forEach(
                            row -> {
                              int id = row.getInt(3);
                              assertTrue(
                                  id >= tableIndex * recordsPerTable
                                      && id < (tableIndex + 1) * recordsPerTable);
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
          }
        });
  }

  /**
   * Five on-disk Hadoop tables with 100 rows each, spread across five partition values {@code
   * p=0..4} (20 rows per partition). Same data columns as {@link #testFiveTablesMiniE2e}.
   */
  @Test
  void testPartitionsMiniE2e() {
    final int numTables = 5;
    final int recordsPerTable = 100;
    final int numPartitions = 5;
    final int recordsPerPartition = recordsPerTable / numPartitions;
    final int rounds = numTables * recordsPerTable;

    withTempDir(
        baseDir -> {
          String[] tablePaths = new String[numTables];
          for (int i = 0; i < numTables; i++) {
            tablePaths[i] = new File(baseDir, "t" + i).getAbsolutePath();
          }

          RowType flinkSchemaPartitioned = wideMiniE2eFlinkSchemaPartitioned();
          StructType deltaSchemaDataOnly =
              Conversions.FlinkToDelta.schema(wideMiniE2eFlinkSchema());
          String schemaJson =
              DataTypeJsonSerDe.serializeStructType(
                  Conversions.FlinkToDelta.schema(flinkSchemaPartitioned));

          DeltaDynamicSink sink =
              DeltaDynamicSink.builder()
                  .withTableProvider(new HadoopTableProvider(Collections.emptyMap()))
                  .withConf(Collections.emptyMap())
                  .build();

          SerializableFunction<Integer, String> supplier = String::valueOf;
          SerializableFunction<String, DynamicRow> parser =
              value -> {
                int idx = Integer.parseInt(value);
                int tableIdx = idx / recordsPerTable;
                int withinTable = idx % recordsPerTable;
                String p = String.valueOf(withinTable % numPartitions);
                return new SerializableDynamicRow(
                    tablePaths[tableIdx],
                    schemaJson,
                    new String[] {"p"},
                    new String[] {p},
                    wideMiniE2eGenericRowPartitioned(idx, p));
              };

          try {
            runDynamicSink(sink, rounds, supplier, parser);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }

          for (int t = 0; t < numTables; t++) {
            final int tableIndex = t;
            String tablePath = tablePaths[t];
            verifyTableContent(
                tablePath,
                (version, actions, props) -> {
                  List<AddFile> actionList = new ArrayList<>();
                  actions.iterator().forEachRemaining(actionList::add);
                  assertEquals(
                      recordsPerTable,
                      actionList.stream().mapToLong(a -> a.getNumRecords().get()).sum());

                  Map<String, Long> partRecordTotals = new HashMap<>();
                  for (AddFile addFile : actionList) {
                    var pv = addFile.getPartitionValues();
                    assertEquals(1, pv.getSize());
                    assertEquals("p", pv.getKeys().getString(0));
                    String part = pv.getValues().getString(0);
                    partRecordTotals.merge(part, addFile.getNumRecords().get(), Long::sum);
                  }
                  assertEquals(numPartitions, partRecordTotals.size());
                  for (int k = 0; k < numPartitions; k++) {
                    assertEquals(
                        recordsPerPartition, partRecordTotals.get(String.valueOf(k)).longValue());
                  }

                  actionList.forEach(
                      addfile -> {
                        List<Row> rows =
                            readParquet(
                                new File(tablePath).toPath().resolve(addfile.getPath()),
                                deltaSchemaDataOnly);
                        rows.forEach(
                            row -> {
                              int id = row.getInt(3);
                              assertTrue(
                                  id >= tableIndex * recordsPerTable
                                      && id < (tableIndex + 1) * recordsPerTable);
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
                              int withinTable = id % recordsPerTable;
                              assertEquals(
                                  String.valueOf(withinTable % numPartitions),
                                  addfile.getPartitionValues().getValues().getString(0));
                            });
                      });
                });
          }
        });
  }

  private static RowType wideMiniE2eFlinkSchema() {
    return RowType.of(
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
  }

  /** Wide mini-E2E schema plus a string partition column {@code p} (last field). */
  private static RowType wideMiniE2eFlinkSchemaPartitioned() {
    RowType base = wideMiniE2eFlinkSchema();
    int n = base.getFieldCount();
    LogicalType[] types = new LogicalType[n + 1];
    String[] names = new String[n + 1];
    for (int i = 0; i < n; i++) {
      RowType.RowField f = base.getFields().get(i);
      types[i] = f.getType();
      names[i] = f.getName();
    }
    types[n] = new VarCharType(VarCharType.MAX_LENGTH);
    names[n] = "p";
    return RowType.of(types, names);
  }

  private static void fillWideMiniE2e(GenericRowData row, int idx) {
    row.setField(0, idx % 10 == 0);
    row.setField(1, (byte) (idx % 8));
    row.setField(2, (short) idx);
    row.setField(3, idx);
    row.setField(4, Long.MAX_VALUE - idx);
    row.setField(5, 1.5f * idx);
    row.setField(6, 3.14159d * idx);
    var decimal = DecimalData.fromBigDecimal(new java.math.BigDecimal("12345.678900"), 18, 6);
    row.setField(7, decimal);
    row.setField(8, StringData.fromString("char" + idx));
    row.setField(9, StringData.fromString("varchar" + idx));
    row.setField(10, StringData.fromString("string" + idx));
    row.setField(11, new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
    row.setField(12, new byte[] {9, 10, 11});
    row.setField(13, new byte[] {42});
    int days = (int) LocalDate.of(2026, 1, 19).toEpochDay();
    row.setField(14, days);
    int millisOfDay = 12 * 60 * 60 * 1000;
    row.setField(15, millisOfDay);
    var ts = TimestampData.fromLocalDateTime(LocalDateTime.of(2026, 1, 19, 12, 0));
    row.setField(16, ts);
    long epochMillis =
        LocalDateTime.of(2026, 1, 19, 12, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
    row.setField(17, TimestampData.fromEpochMillis(epochMillis));
    int months = 2 * 12 + 3;
    row.setField(18, months);
    long millis = 3L * 24 * 60 * 60 * 1000;
    row.setField(19, millis);
  }

  private static GenericRowData wideMiniE2eGenericRow(int idx) {
    GenericRowData row = new GenericRowData(20);
    fillWideMiniE2e(row, idx);
    return row;
  }

  private static GenericRowData wideMiniE2eGenericRowPartitioned(int idx, String partitionValue) {
    GenericRowData row = new GenericRowData(21);
    fillWideMiniE2e(row, idx);
    row.setField(20, StringData.fromString(partitionValue));
    return row;
  }

  /**
   * Serializable {@link DeltaTableProvider} for serialization test only (never used at runtime in
   * that test).
   */
  private static final class ThrowingSerializableProvider implements DeltaTableProvider {
    @Override
    public DeltaTable getOrCreate(
        String tableName, StructType schema, List<String> partitionColumns) {
      throw new UnsupportedOperationException("test placeholder");
    }
  }
}
