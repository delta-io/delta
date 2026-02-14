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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.flink.TestHelper;
import io.delta.flink.table.HadoopTable;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import org.apache.flink.table.data.*;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

/** JUnit 6 test suite for DeltaWriterTask. */
class DeltaWriterTaskTest extends TestHelper {

  @Test
  void testWriteToEmptyTable() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

          HadoopTable table =
              new HadoopTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, List.of("part"));
          table.open();

          Map<String, io.delta.kernel.expressions.Literal> partitionValues =
              Map.of("part", io.delta.kernel.expressions.Literal.ofString("p0"));

          DeltaSinkConf conf = new DeltaSinkConf(schema, Collections.emptyMap());
          DeltaWriterTask writerTask =
              new DeltaWriterTask(
                  /* jobId= */ "test-job-id",
                  /* subtaskId= */ 2,
                  /* attemptNumber= */ 0,
                  /* table = */ table,
                  /* conf = */ conf,
                  /* partitionValues= */ partitionValues);

          for (int i = 0; i < 10; i++) {
            writerTask.write(
                GenericRowData.of(Integer.valueOf(i), StringData.fromString("p0")),
                new TestSinkWriterContext(i * 100, i * 100));
          }
          List<DeltaWriterResult> results = writerTask.complete();

          assertEquals(1, results.size());
          results.forEach(
              result -> {
                assertEquals(1, result.getDeltaActions().size());
                Row action = result.getDeltaActions().get(0);
                AddFile addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL));
                assertTrue(addFile.getPath().contains("test-job-id-2-0"));
                // Stats are present
                assertEquals(10, addFile.getNumRecords().get());
                Path fullPath = dir.toPath().resolve(addFile.getPath()).toAbsolutePath();
                assertTrue(Files.exists(fullPath));

                var partitionMap = addFile.getPartitionValues();
                assertEquals(1, partitionMap.getSize());
                assertEquals("part", partitionMap.getKeys().getString(0));
                assertEquals("p0", partitionMap.getValues().getString(0));

                assertEquals(900, result.getContext().getHighWatermark());
                assertEquals(0, result.getContext().getLowWatermark());
                // check the Parquet file content
                List<Row> rows = readParquet(fullPath, schema);
                assertEquals(10, rows.size());
                for (int idx = 0; idx < rows.size(); idx++) {
                  assertEquals(idx, rows.get(idx).getInt(0));
                }
              });
        });
  }

  @Test
  void testWriteToExistingTable() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

          HadoopTable table =
              new HadoopTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, List.of("part"));
          table.open();
          Map<String, io.delta.kernel.expressions.Literal> partitionValues =
              Map.of("part", io.delta.kernel.expressions.Literal.ofString("p0"));
          DeltaSinkConf conf = new DeltaSinkConf(schema, Collections.emptyMap());
          DeltaWriterTask writerTask =
              new DeltaWriterTask(
                  /* jobId= */ "test-job-id",
                  /* subtaskId= */ 2,
                  /* attemptNumber= */ 0,
                  /* table = */ table,
                  /* conf = */ conf,
                  /* partitionValues= */ partitionValues);

          for (int i = 0; i < 10; i++) {
            writerTask.write(
                GenericRowData.of(Integer.valueOf(i), StringData.fromString("p0")),
                new TestSinkWriterContext(i * 100, i * 100));
          }
          List<DeltaWriterResult> results = writerTask.complete();

          assertEquals(1, results.size());
          results.forEach(
              result -> {
                assertEquals(1, result.getDeltaActions().size());
                Row action = result.getDeltaActions().get(0);
                AddFile addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL));
                assertTrue(addFile.getPath().contains("test-job-id-2-0"));
                assertEquals(10, addFile.getNumRecords().get());
                Path fullPath = dir.toPath().resolve(addFile.getPath()).toAbsolutePath();
                assertTrue(Files.exists(fullPath));

                assertEquals(900, result.getContext().getHighWatermark());
                assertEquals(0, result.getContext().getLowWatermark());

                var partitionMap = addFile.getPartitionValues();
                assertEquals(1, partitionMap.getSize());
                assertEquals("part", partitionMap.getKeys().getString(0));
                assertEquals("p0", partitionMap.getValues().getString(0));

                // check the Parquet file content
                List<Row> rows = readParquet(fullPath, schema);
                assertEquals(10, rows.size());
                for (int idx = 0; idx < rows.size(); idx++) {
                  assertEquals(idx, rows.get(idx).getInt(0));
                }
              });
        });
  }

  @Test
  void testWritePrimitiveTypes() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          StructType schema =
              new StructType()
                  .add("i", IntegerType.INTEGER)
                  .add("b", BooleanType.BOOLEAN)
                  .add("s", StringType.STRING)
                  .add("bin", BinaryType.BINARY)
                  .add("l", LongType.LONG)
                  .add("f", FloatType.FLOAT)
                  .add("d", DoubleType.DOUBLE)
                  .add("dec", new DecimalType(10, 2))
                  .add("dt", DateType.DATE)
                  .add("ts", TimestampType.TIMESTAMP)
                  .add("tsn", TimestampNTZType.TIMESTAMP_NTZ);

          HadoopTable table =
              new HadoopTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, Collections.emptyList());
          table.open();
          Map<String, io.delta.kernel.expressions.Literal> partitionValues = Collections.emptyMap();

          DeltaSinkConf conf = new DeltaSinkConf(schema, Collections.emptyMap());
          DeltaWriterTask writerTask =
              new DeltaWriterTask(
                  /* jobId= */ "test-job-id",
                  /* subtaskId= */ 2,
                  /* attemptNumber= */ 0,
                  /* table = */ table,
                  /* conf = */ conf,
                  /* partitionValues= */ partitionValues);
          int numRecords = 15;
          for (int i = 0; i < numRecords; i++) {
            if (i % 10 == 0) {
              writerTask.write(
                  GenericRowData.of(
                      null, null, null, null, null, null, null, null, null, null, null),
                  new TestSinkWriterContext(i * 100, i * 100));
            } else {
              writerTask.write(
                  GenericRowData.of(
                      i,
                      i % 2 == 0,
                      StringData.fromString("p" + i),
                      ("binary data " + i).getBytes(StandardCharsets.UTF_8),
                      100L + i,
                      3.75f + i,
                      4.28 + i,
                      DecimalData.fromBigDecimal(new BigDecimal("" + i + "3.17"), 10, 2),
                      (int) LocalDate.of(2025, 10, 10 + i).toEpochDay(),
                      TimestampData.fromInstant(
                          Instant.parse("2025-10-" + (10 + i) + "T00:00:00Z")),
                      TimestampData.fromLocalDateTime(LocalDateTime.of(2025, 10, 10, i, 0, 0))),
                  new TestSinkWriterContext(i * 100, i * 100));
            }
          }
          Collection<DeltaWriterResult> results = writerTask.complete();

          assertEquals(1, results.size());
          results.forEach(
              result -> {
                Row action = result.getDeltaActions().get(0);
                AddFile addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL));
                Path fullPath = dir.toPath().resolve(addFile.getPath()).toAbsolutePath();

                // check the Parquet file content
                List<Row> rows = readParquet(fullPath, schema);
                assertEquals(numRecords, rows.size());
                for (int idx = 0; idx < rows.size(); idx++) {
                  Row row = rows.get(idx);
                  if (idx % 10 == 0) {
                    for (int j = 0; j < 11; j++) {
                      assertTrue(row.isNullAt(j));
                    }
                  } else {
                    assertEquals(idx, row.getInt(0));
                    assertEquals((idx % 2 == 0), row.getBoolean(1));
                    assertEquals("p" + idx, row.getString(2));
                    assertEquals(
                        "binary data " + idx, new String(row.getBinary(3), StandardCharsets.UTF_8));
                    assertEquals(100L + idx, row.getLong(4));
                    assertEquals(3.75f + idx, row.getFloat(5), 0.001);
                    assertEquals(4.28 + idx, row.getDouble(6), 0.001);
                    assertEquals(
                        new BigDecimal("" + idx + "3.17")
                            .setScale(2, RoundingMode.HALF_UP)
                            .round(new MathContext(10, RoundingMode.HALF_UP)),
                        row.getDecimal(7));
                    assertEquals(
                        (int) LocalDate.of(2025, 10, 10 + idx).toEpochDay(), row.getInt(8));
                    assertEquals(
                        Instant.parse("2025-10-" + (10 + idx) + "T00:00:00Z").toEpochMilli() * 1000,
                        row.getLong(9));
                    assertEquals(
                        LocalDateTime.of(2025, 10, 10, idx, 0, 0)
                                .toInstant(ZonedDateTime.now(ZoneId.of("UTC")).getOffset())
                                .toEpochMilli()
                            * 1000,
                        row.getLong(10));
                  }
                }
              });
        });
  }

  @Test
  void testWriteNestedStructTypes() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          StructType nestedStructType =
              new StructType()
                  .add("nested_id", IntegerType.INTEGER)
                  .add("nested_name", StringType.STRING);
          StructType nestedStructType2 =
              new StructType()
                  .add("nested_sth", IntegerType.INTEGER)
                  .add("nested2", nestedStructType);
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("nested", nestedStructType2);

          HadoopTable table =
              new HadoopTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, Collections.emptyList());
          table.open();
          Map<String, io.delta.kernel.expressions.Literal> partitionValues = Collections.emptyMap();

          DeltaSinkConf conf = new DeltaSinkConf(schema, Collections.emptyMap());
          DeltaWriterTask writerTask =
              new DeltaWriterTask(
                  /* jobId= */ "test-job-id",
                  /* subtaskId= */ 2,
                  /* attemptNumber= */ 0,
                  /* table = */ table,
                  /* conf = */ conf,
                  /* partitionValues= */ partitionValues);

          int numRecords = 15;
          for (int i = 0; i < numRecords; i++) {
            if (i % 4 == 0) {
              writerTask.write(
                  GenericRowData.of(i, null), new TestSinkWriterContext(i * 100, i * 100));
            } else if (i % 7 == 0) {
              writerTask.write(
                  GenericRowData.of(i, GenericRowData.of(i, null)),
                  new TestSinkWriterContext(i * 100, i * 100));
            } else {
              writerTask.write(
                  GenericRowData.of(
                      i,
                      GenericRowData.of(i, GenericRowData.of(i, StringData.fromString("p" + i)))),
                  new TestSinkWriterContext(i * 100, i * 100));
            }
          }
          Collection<DeltaWriterResult> results = writerTask.complete();

          assertEquals(1, results.size());
          results.forEach(
              result -> {
                Row action = result.getDeltaActions().get(0);
                AddFile addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL));
                Path fullPath = dir.toPath().resolve(addFile.getPath()).toAbsolutePath();

                // check the Parquet file content
                List<Row> rows = readParquet(fullPath, schema);
                assertEquals(numRecords, rows.size());
                for (int idx = 0; idx < rows.size(); idx++) {
                  Row row = rows.get(idx);
                  assertEquals(idx, row.getInt(0));
                  if (idx % 4 == 0) {
                    assertTrue(row.isNullAt(1));
                  } else if (idx % 7 == 0) {
                    assertEquals(idx, row.getStruct(1).getInt(0));
                    assertTrue(row.getStruct(1).isNullAt(1));
                  } else {
                    assertEquals(idx, row.getStruct(1).getInt(0));
                    assertEquals(idx, row.getStruct(1).getStruct(1).getInt(0));
                    assertEquals("p" + idx, row.getStruct(1).getStruct(1).getString(1));
                  }
                }
              });
        });
  }

  @Test
  void testWriteList() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          StructType nestedStructType =
              new StructType()
                  .add("nested_id", IntegerType.INTEGER)
                  .add("nested_name", StringType.STRING)
                  .add("nested_list", new ArrayType(IntegerType.INTEGER, true));
          StructType schema =
              new StructType()
                  .add("id", IntegerType.INTEGER)
                  .add("intlist", new ArrayType(IntegerType.INTEGER, true))
                  .add("nestedlist", new ArrayType(nestedStructType, true));

          HadoopTable table =
              new HadoopTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, Collections.emptyList());
          table.open();
          Map<String, io.delta.kernel.expressions.Literal> partitionValues = Collections.emptyMap();

          DeltaSinkConf conf = new DeltaSinkConf(schema, Collections.emptyMap());
          DeltaWriterTask writerTask =
              new DeltaWriterTask(
                  /* jobId= */ "test-job-id",
                  /* subtaskId= */ 2,
                  /* attemptNumber= */ 0,
                  /* table = */ table,
                  /* conf = */ conf,
                  /* partitionValues= */ partitionValues);

          int numRecords = 15;
          for (int i = 0; i < numRecords; i++) {
            writerTask.write(
                GenericRowData.of(
                    i,
                    new GenericArrayData(
                        new Object[] {Integer.valueOf(1), null, Integer.valueOf(3 * i)}),
                    new GenericArrayData(
                        new Object[] {
                          GenericRowData.of(100 + i, StringData.fromString("1p" + i), null),
                          null,
                          GenericRowData.of(
                              300 + i,
                              StringData.fromString("3p" + i),
                              new GenericArrayData(
                                  new Object[] {
                                    Integer.valueOf(i),
                                    Integer.valueOf(i + 1),
                                    null,
                                    Integer.valueOf(4 * i)
                                  }))
                        })),
                new TestSinkWriterContext(i * 100, i * 100));
          }
          List<DeltaWriterResult> results = writerTask.complete();

          assertEquals(1, results.size());
          results.forEach(
              result -> {
                Row action = result.getDeltaActions().get(0);
                AddFile addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL));
                Path fullPath = dir.toPath().resolve(addFile.getPath()).toAbsolutePath();

                // check the Parquet file content
                List<Row> rows = readParquet(fullPath, schema);
                assertEquals(numRecords, rows.size());
                for (int idx = 0; idx < rows.size(); idx++) {
                  Row row = rows.get(idx);
                  assertEquals(idx, row.getInt(0));
                  assertEquals(3, row.getArray(1).getSize());
                  assertEquals(1, row.getArray(1).getElements().getInt(0));
                  assertTrue(row.getArray(1).getElements().isNullAt(1));
                  assertEquals(3 * idx, row.getArray(1).getElements().getInt(2));

                  assertEquals(3, row.getArray(2).getSize());
                  var elements = row.getArray(2).getElements();

                  assertEquals(100 + idx, elements.getChild(0).getInt(0));
                  assertEquals("1p" + idx, elements.getChild(1).getString(0));
                  assertTrue(elements.getChild(2).isNullAt(0));

                  assertTrue(elements.getChild(0).isNullAt(1));
                  assertTrue(elements.getChild(1).isNullAt(1));
                  assertTrue(elements.getChild(2).isNullAt(1));

                  assertEquals(300 + idx, elements.getChild(0).getInt(2));
                  assertEquals("3p" + idx, elements.getChild(1).getString(2));
                  var subarray = elements.getChild(2).getArray(2);
                  assertEquals(4, subarray.getSize());
                  assertEquals(idx, subarray.getElements().getInt(0));
                  assertEquals(idx + 1, subarray.getElements().getInt(1));
                  assertTrue(subarray.getElements().isNullAt(2));
                  assertEquals(idx * 4, subarray.getElements().getInt(3));
                }
              });
        });
  }

  @Test
  void testWriteListOfAllTypes() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          StructType schema =
              new StructType()
                  .add("i", new ArrayType(IntegerType.INTEGER, true))
                  .add("b", new ArrayType(BooleanType.BOOLEAN, true))
                  .add("s", new ArrayType(StringType.STRING, true))
                  .add("bin", new ArrayType(BinaryType.BINARY, true))
                  .add("l", new ArrayType(LongType.LONG, true))
                  .add("f", new ArrayType(FloatType.FLOAT, true))
                  .add("d", new ArrayType(DoubleType.DOUBLE, true))
                  .add("dec", new ArrayType(new DecimalType(10, 2), true))
                  .add("dt", new ArrayType(DateType.DATE, true))
                  .add("ts", new ArrayType(TimestampType.TIMESTAMP, true))
                  .add("tsn", new ArrayType(TimestampNTZType.TIMESTAMP_NTZ, true));

          HadoopTable table =
              new HadoopTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, Collections.emptyList());
          table.open();
          Map<String, io.delta.kernel.expressions.Literal> partitionValues = Collections.emptyMap();

          DeltaSinkConf conf = new DeltaSinkConf(schema, Collections.emptyMap());
          DeltaWriterTask writerTask =
              new DeltaWriterTask(
                  /* jobId= */ "test-job-id",
                  /* subtaskId= */ 2,
                  /* attemptNumber= */ 0,
                  /* table = */ table,
                  /* conf = */ conf,
                  /* partitionValues= */ partitionValues);
          int numRecords = 15;
          for (int i = 0; i < numRecords; i++) {
            writerTask.write(
                GenericRowData.of(
                    new GenericArrayData(new Object[] {i}),
                    new GenericArrayData(new Object[] {i % 2 == 0}),
                    new GenericArrayData(new Object[] {StringData.fromString("p" + i)}),
                    new GenericArrayData(
                        new Object[] {("binary data " + i).getBytes(StandardCharsets.UTF_8)}),
                    new GenericArrayData(new Object[] {100L + i}),
                    new GenericArrayData(new Object[] {3.75f + i}),
                    new GenericArrayData(new Object[] {4.28 + i}),
                    new GenericArrayData(
                        new Object[] {
                          DecimalData.fromBigDecimal(new BigDecimal("" + i + "3.17"), 10, 2)
                        }),
                    new GenericArrayData(
                        new Object[] {(int) LocalDate.of(2025, 10, 10 + i).toEpochDay()}),
                    new GenericArrayData(
                        new Object[] {
                          TimestampData.fromInstant(
                              Instant.parse("2025-10-" + (10 + i) + "T00:00:00Z"))
                        }),
                    new GenericArrayData(
                        new Object[] {
                          TimestampData.fromLocalDateTime(LocalDateTime.of(2025, 10, 10, i, 0, 0))
                        })),
                new TestSinkWriterContext(i * 100, i * 100));
          }
          List<DeltaWriterResult> results = writerTask.complete();

          assertEquals(1, results.size());
          results.forEach(
              result -> {
                Row action = result.getDeltaActions().get(0);
                AddFile addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL));
                Path fullPath = dir.toPath().resolve(addFile.getPath()).toAbsolutePath();

                // check the Parquet file content
                List<Row> rows = readParquet(fullPath, schema);
                assertEquals(numRecords, rows.size());
                for (int idx = 0; idx < rows.size(); idx++) {
                  Row row = rows.get(idx);
                  assertEquals(idx, row.getArray(0).getElements().getInt(0));
                  assertEquals((idx % 2 == 0), row.getArray(1).getElements().getBoolean(0));
                  assertEquals("p" + idx, row.getArray(2).getElements().getString(0));
                  assertEquals(
                      "binary data " + idx,
                      new String(
                          row.getArray(3).getElements().getBinary(0), StandardCharsets.UTF_8));
                  assertEquals(100L + idx, row.getArray(4).getElements().getLong(0));
                  assertEquals(3.75f + idx, row.getArray(5).getElements().getFloat(0), 0.001);
                  assertEquals(4.28 + idx, row.getArray(6).getElements().getDouble(0), 0.001);
                  assertEquals(
                      new BigDecimal("" + idx + "3.17")
                          .setScale(2, RoundingMode.HALF_UP)
                          .round(new MathContext(10, RoundingMode.HALF_UP)),
                      row.getArray(7).getElements().getDecimal(0));
                  assertEquals(
                      (int) LocalDate.of(2025, 10, 10 + idx).toEpochDay(),
                      row.getArray(8).getElements().getInt(0));
                  assertEquals(
                      Instant.parse("2025-10-" + (10 + idx) + "T00:00:00Z").toEpochMilli() * 1000,
                      row.getArray(9).getElements().getLong(0));
                  assertEquals(
                      LocalDateTime.of(2025, 10, 10, idx, 0, 0)
                              .toInstant(ZonedDateTime.now(ZoneId.of("UTC")).getOffset())
                              .toEpochMilli()
                          * 1000,
                      row.getArray(10).getElements().getLong(0));
                }
              });
        });
  }

  @Test
  void testWriteMap() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          StructType nestedStructType =
              new StructType()
                  .add("nested_id", IntegerType.INTEGER)
                  .add("nested_name", StringType.STRING)
                  .add("nested_map", new MapType(IntegerType.INTEGER, StringType.STRING, true));
          StructType schema =
              new StructType()
                  .add("id", IntegerType.INTEGER)
                  .add("simplemap", new MapType(IntegerType.INTEGER, StringType.STRING, true))
                  .add("structmap", new MapType(IntegerType.INTEGER, nestedStructType, true));

          HadoopTable table =
              new HadoopTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, Collections.emptyList());
          table.open();
          Map<String, io.delta.kernel.expressions.Literal> partitionValues = Collections.emptyMap();

          DeltaSinkConf conf = new DeltaSinkConf(schema, Collections.emptyMap());
          DeltaWriterTask writerTask =
              new DeltaWriterTask(
                  /* jobId= */ "test-job-id",
                  /* subtaskId= */ 2,
                  /* attemptNumber= */ 0,
                  /* table = */ table,
                  /* conf = */ conf,
                  /* partitionValues= */ partitionValues);

          int numRecords = 15;
          for (int i = 0; i < numRecords; i++) {
            Map<Object, Object> simpleMap = new LinkedHashMap<>();
            simpleMap.put(i, StringData.fromString("p" + i));
            simpleMap.put(i + 1, StringData.fromString("q" + i));
            simpleMap.put(i + 2, null);
            simpleMap.put(i + 3, StringData.fromString("w" + i));

            Map<Object, Object> structMap = new LinkedHashMap<>();
            structMap.put(i, GenericRowData.of(i + 100, StringData.fromString("n" + i), null));
            structMap.put(i + 1, null);
            structMap.put(
                i + 2,
                GenericRowData.of(
                    i + 200,
                    StringData.fromString("m" + i),
                    new GenericMapData(Map.of(i, StringData.fromString("pwd" + i)))));

            writerTask.write(
                GenericRowData.of(i, new GenericMapData(simpleMap), new GenericMapData(structMap)),
                new TestSinkWriterContext(i * 100, i * 100));
          }
          List<DeltaWriterResult> results = writerTask.complete();

          assertEquals(1, results.size());
          results.forEach(
              result -> {
                Row action = result.getDeltaActions().get(0);
                AddFile addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL));
                Path fullPath = dir.toPath().resolve(addFile.getPath()).toAbsolutePath();

                // check the Parquet file content
                List<Row> rows = readParquet(fullPath, schema);
                assertEquals(numRecords, rows.size());
                for (int idx = 0; idx < rows.size(); idx++) {
                  Row row = rows.get(idx);
                  assertEquals(idx, row.getInt(0));

                  assertEquals(4, row.getMap(1).getSize());
                  assertEquals(idx, row.getMap(1).getKeys().getInt(0));
                  assertEquals(idx + 1, row.getMap(1).getKeys().getInt(1));
                  assertEquals(idx + 2, row.getMap(1).getKeys().getInt(2));
                  assertEquals(idx + 3, row.getMap(1).getKeys().getInt(3));
                  assertEquals("p" + idx, row.getMap(1).getValues().getString(0));
                  assertEquals("q" + idx, row.getMap(1).getValues().getString(1));
                  assertTrue(row.getMap(1).getValues().isNullAt(2));
                  assertEquals("w" + idx, row.getMap(1).getValues().getString(3));

                  assertEquals(3, row.getMap(2).getSize());
                  var keys = row.getMap(2).getKeys();
                  var values = row.getMap(2).getValues();

                  assertEquals(idx, keys.getInt(0));
                  assertEquals(idx + 1, keys.getInt(1));
                  assertEquals(idx + 2, keys.getInt(2));
                  assertEquals(idx + 100, values.getChild(0).getInt(0));
                  assertTrue(values.getChild(0).isNullAt(1));
                  assertEquals(idx + 200, values.getChild(0).getInt(2));

                  assertEquals("n" + idx, values.getChild(1).getString(0));
                  assertTrue(values.getChild(1).isNullAt(1));
                  assertEquals("m" + idx, values.getChild(1).getString(2));

                  assertTrue(values.getChild(2).isNullAt(0));
                  assertTrue(values.getChild(2).isNullAt(1));
                  assertEquals(1, values.getChild(2).getMap(2).getSize());
                  assertEquals(idx, values.getChild(2).getMap(2).getKeys().getInt(0));
                  assertEquals("pwd" + idx, values.getChild(2).getMap(2).getValues().getString(0));
                }
              });
        });
  }

  @Test
  void testWriteComplexCombination() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          StructType nestedStructType =
              new StructType()
                  .add(
                      "nested_map",
                      new MapType(
                          IntegerType.INTEGER, new ArrayType(StringType.STRING, true), true));
          StructType schema =
              new StructType()
                  .add(
                      "base",
                      new ArrayType(
                          new MapType(IntegerType.INTEGER, nestedStructType, true), true));

          HadoopTable table =
              new HadoopTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, Collections.emptyList());
          table.open();
          Map<String, io.delta.kernel.expressions.Literal> partitionValues = Collections.emptyMap();

          DeltaSinkConf conf = new DeltaSinkConf(schema, Collections.emptyMap());
          DeltaWriterTask writerTask =
              new DeltaWriterTask(
                  /* jobId= */ "test-job-id",
                  /* subtaskId= */ 2,
                  /* attemptNumber= */ 0,
                  /* table = */ table,
                  /* conf = */ conf,
                  /* partitionValues= */ partitionValues);

          int numRecords = 15;
          for (int i = 0; i < numRecords; i++) {
            Map<Object, Object> map1 =
                Map.of(
                    i,
                    GenericRowData.of(
                        new GenericMapData(
                            Map.of(
                                i,
                                new GenericArrayData(
                                    new Object[] {
                                      StringData.fromString("p" + i), StringData.fromString("q" + i)
                                    })))));

            Map<Object, Object> map2 =
                Map.of(
                    i + 1,
                    GenericRowData.of(
                        new GenericMapData(
                            Map.of(
                                i,
                                new GenericArrayData(
                                    new Object[] {StringData.fromString("w" + i)})))));

            writerTask.write(
                GenericRowData.of(
                    new GenericArrayData(
                        new Object[] {new GenericMapData(map1), new GenericMapData(map2)})),
                new TestSinkWriterContext(i * 100, i * 100));
          }
          List<DeltaWriterResult> results = writerTask.complete();

          assertEquals(1, results.size());
          results.forEach(
              result -> {
                Row action = result.getDeltaActions().get(0);
                AddFile addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL));
                Path fullPath = dir.toPath().resolve(addFile.getPath()).toAbsolutePath();

                // check the Parquet file content
                List<Row> rows = readParquet(fullPath, schema);
                assertEquals(numRecords, rows.size());
                for (int idx = 0; idx < rows.size(); idx++) {
                  Row row = rows.get(idx);
                  assertEquals(2, row.getArray(0).getSize());
                  var map1 = row.getArray(0).getElements().getMap(0);
                  var map2 = row.getArray(0).getElements().getMap(1);

                  assertEquals(1, map1.getSize());
                  assertEquals(idx, map1.getKeys().getInt(0));
                  assertEquals(1, map1.getValues().getChild(0).getMap(0).getSize());
                  assertEquals(idx, map1.getValues().getChild(0).getMap(0).getKeys().getInt(0));
                  assertEquals(
                      2, map1.getValues().getChild(0).getMap(0).getValues().getArray(0).getSize());
                  var values = map1.getValues().getChild(0).getMap(0).getValues();
                  assertEquals("p" + idx, values.getArray(0).getElements().getString(0));
                  assertEquals("q" + idx, values.getArray(0).getElements().getString(1));

                  assertEquals(1, map2.getSize());
                  assertEquals(idx + 1, map2.getKeys().getInt(0));
                  assertEquals(1, map2.getValues().getChild(0).getMap(0).getSize());
                  assertEquals(idx, map2.getValues().getChild(0).getMap(0).getKeys().getInt(0));
                  assertEquals(
                      1, map2.getValues().getChild(0).getMap(0).getValues().getArray(0).getSize());
                  var values2 = map2.getValues().getChild(0).getMap(0).getValues();
                  assertEquals("w" + idx, values2.getArray(0).getElements().getString(0));
                }
              });
        });
  }

  @Test
  void testFileRollingBySize() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          RowType flinkSchema =
              RowType.of(
                  new org.apache.flink.table.types.logical.LogicalType[] {
                    new IntType(), new VarCharType(VarCharType.MAX_LENGTH)
                  },
                  new String[] {"id", "part"});
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

          HadoopTable table =
              new HadoopTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, Collections.emptyList());
          table.open();
          Map<String, io.delta.kernel.expressions.Literal> partitionValues = Collections.emptyMap();

          int rollSize = 500;
          int rowCount = 1000;

          DeltaSinkConf conf =
              new DeltaSinkConf(
                  schema,
                  Map.of(
                      "file_rolling.strategy",
                      "size",
                      "file_rolling.size",
                      String.valueOf(rollSize)));
          DeltaWriterTask writerTask =
              new DeltaWriterTask(
                  /* jobId= */ "test-job-id",
                  /* subtaskId= */ 2,
                  /* attemptNumber= */ 0,
                  /* table = */ table,
                  /* conf = */ conf,
                  /* partitionValues= */ partitionValues);

          int fileCounter = 0;
          long sizeCounter = 0L;

          for (int i = 0; i < rowCount; i++) {
            var binaryRow =
                new RowDataSerializer(flinkSchema)
                    .toBinaryRow(GenericRowData.of(i, StringData.fromString("p" + i)));
            sizeCounter += binaryRow.getSizeInBytes();
            if (sizeCounter >= rollSize) {
              sizeCounter = 0;
              fileCounter += 1;
            }
            writerTask.write(binaryRow, new TestSinkWriterContext(i * 100, i * 100));
          }
          List<DeltaWriterResult> results = writerTask.complete();

          if (sizeCounter != 0) {
            fileCounter += 1;
          }

          assertEquals(fileCounter, results.size());
          Set<Integer> ids = new HashSet<>();
          for (DeltaWriterResult result : results) {
            assertEquals(1, result.getDeltaActions().size());
            Row action = result.getDeltaActions().get(0);
            AddFile addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL));
            Path fullPath = dir.toPath().resolve(addFile.getPath()).toAbsolutePath();
            // check the Parquet file content
            List<Row> rows = readParquet(fullPath, schema);
            for (Row row : rows) {
              ids.add(row.getInt(0));
            }
          }
          assertEquals(rowCount, ids.size());
        });
  }

  @Test
  void testFileRollingByCount() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

          HadoopTable table =
              new HadoopTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, Collections.emptyList());
          table.open();
          Map<String, io.delta.kernel.expressions.Literal> partitionValues = Collections.emptyMap();

          int rollSize = 200;
          int rowCount = 1001;

          DeltaSinkConf conf =
              new DeltaSinkConf(
                  schema,
                  Map.of(
                      "file_rolling.strategy",
                      "count",
                      "file_rolling.count",
                      String.valueOf(rollSize)));
          DeltaWriterTask writerTask =
              new DeltaWriterTask(
                  /* jobId= */ "test-job-id",
                  /* subtaskId= */ 2,
                  /* attemptNumber= */ 0,
                  /* table = */ table,
                  /* conf = */ conf,
                  /* partitionValues= */ partitionValues);

          for (int i = 0; i < rowCount; i++) {
            writerTask.write(
                GenericRowData.of(i, StringData.fromString("p0")),
                new TestSinkWriterContext(i * 100, i * 100));
          }
          List<DeltaWriterResult> results = writerTask.complete();

          int expectedFileCount = rowCount / rollSize + 1;
          assertEquals(expectedFileCount, results.size());
          Set<Integer> ids = new HashSet<>();
          for (int idx = 0; idx < results.size(); idx++) {
            DeltaWriterResult result = results.get(idx);
            assertEquals(1, result.getDeltaActions().size());
            Row action = result.getDeltaActions().get(0);
            AddFile addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL));
            Path fullPath = dir.toPath().resolve(addFile.getPath()).toAbsolutePath();

            assertEquals(
                Math.min((rowCount - 1) * 100, (((idx + 1) * rollSize) - 1) * 100),
                result.getContext().getHighWatermark());
            assertEquals(0, result.getContext().getLowWatermark());

            // check the Parquet file content
            List<Row> rows = readParquet(fullPath, schema);
            for (Row row : rows) {
              ids.add(row.getInt(0));
            }
          }
          assertEquals(rowCount, ids.size());
        });
  }
}
