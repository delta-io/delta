/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.flink.sink;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.hamcrest.core.IsEqual;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;

public class DeltaSinkWriteReadITCase {

    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private String deltaTablePath;

    @BeforeAll
    public static void beforeAll() throws IOException {
        TEMPORARY_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        TEMPORARY_FOLDER.delete();
    }

    @BeforeEach
    public void setup() throws IOException {
        deltaTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
    }

    @Test
    public void testWriteReadToDeltaTable() throws Exception {
        // GIVEN
        RowType rowType = new RowType(
            Arrays.asList(
                new RowType.RowField("f1", new FloatType()),
                new RowType.RowField("f2", new IntType()),
                new RowType.RowField("f3", new VarCharType()),
                new RowType.RowField("f4", new DoubleType()),
                new RowType.RowField("f5", new BooleanType()),
                new RowType.RowField("f6", new TinyIntType()),
                new RowType.RowField("f7", new SmallIntType()),
                new RowType.RowField("f8", new BigIntType()),
                new RowType.RowField("f9", new BinaryType()),
                new RowType.RowField("f10", new VarBinaryType()),
                new RowType.RowField("f11", new TimestampType()),
                new RowType.RowField("f12", new LocalZonedTimestampType()),
                new RowType.RowField("f13", new DateType()),
                new RowType.RowField("f14", new CharType()),
                new RowType.RowField("f15", new DecimalType()),
                new RowType.RowField("f16", new DecimalType(4, 2))
            ));
        Integer value = 1;
        Row testRow = Row.of(
            value.floatValue(), // float type
            value, // int type
            value.toString(), // varchar type
            value.doubleValue(), // double type
            false, // boolean type
            value.byteValue(), // tiny int type
            value.shortValue(), // small int type
            value.longValue(), // big int type
            String.valueOf(value).getBytes(StandardCharsets.UTF_8), // binary type
            String.valueOf(value).getBytes(StandardCharsets.UTF_8), // varbinary type
            LocalDateTime.now(ZoneOffset.systemDefault()), // timestamp type
            Instant.now(), // local zoned timestamp type
            LocalDate.now(), // date type
            String.valueOf(value), // char type
            BigDecimal.valueOf(value), // decimal type
            new BigDecimal("11.11") // decimal(4,2) type
        );

        // WHEN
        runFlinkJobInBackground(rowType, rowToRowData(rowType, testRow));

        // THEN
        DeltaLog deltaLog =
            DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), deltaTablePath);
        waitUntilDeltaLogExists(deltaLog);
        validate(deltaLog.snapshot(), testRow);
    }

    @Test
    public void testNestedTypes() throws Throwable {
        // GIVEN
        RowType rowType = new RowType(
            Arrays.asList(
                new RowType.RowField("f1", new MapType(new VarCharType(), new IntType())),
                new RowType.RowField("f2", new ArrayType(new IntType())),
                new RowType.RowField("f3", new RowType(Collections.singletonList(
                    new RowType.RowField("f01", new IntType())
                )))
            ));

        Integer value = 1;
        Integer[] testArray = {value};
        Map<String, Integer> testMap = new HashMap<String, Integer>() {{
                put(String.valueOf(value), value);
            }};

        Row nestedRow = Row.of(value);
        Row testRow = Row.of(testMap, testArray, nestedRow);

        // WHEN
        runFlinkJobInBackground(rowType, rowToRowData(rowType, testRow));

        // THEN
        DeltaLog deltaLog =
            DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), deltaTablePath);
        waitUntilDeltaLogExists(deltaLog);

        validateNestedData(deltaLog.snapshot(), testRow);
    }

    /**
     * This test tries to write a Parquet file with schema {@code ROW<Array<Array<Int>>>} This
     * is expected to fail due to issue in flink-parquet library, were writing complex nested types
     * is still not implemented fully.
     */
    @Test
    public void testNestedComplexTypes_ArrayOfArrays() {
        // GIVEN
        RowType rowType = new RowType(
            Collections.singletonList(
                new RowField("f2", new ArrayType(new ArrayType(new IntType())))
            ));

        int value = 1;
        Integer[] testArray = {value};
        Integer[][] testArrayOfArrays = new Integer[][] {testArray};

        // We need this casting to an Object because Row.of(...) accepts varargs and without the
        // cast, during the runtime an array is interpreted as varargs and not single object
        // making the test fail on rowToRowData(...) before starting Flink Job.
        // The issue is also reported by Intellij code hints:
        // "Confusing argument '(testArrayOfArrays)', unclear if a varargs or non-varargs call is
        // desired. Cast to Object"
        Row testRow = Row.of((Object) testArrayOfArrays);

        // WHEN
        RuntimeException exception = assertThrows(
            RuntimeException.class,
            () -> runFlinkJob(rowType, rowToRowData(rowType, testRow))
        );

        // THEN
        System.out.println(exception.getCause().getCause().getCause().getMessage());

        assertThat(
            exception.getCause().getCause().getCause().getMessage(),
            IsEqual.equalTo(
                "org.apache.parquet.io.ParquetEncodingException: empty fields are illegal,"
                    + " the field should be ommited completely instead")
        );
    }

    /**
     * This test tries to write a Parquet file with schema {@code ROW<Array<Row<Int>>>} This
     * is expected to fail due to issue in flink-parquet library, were writing complex nested types
     * is still not implemented fully.
     */
    @Test
    public void testNestedComplexTypes_ArrayOfRows() {
        // GIVEN
        RowType rowType = new RowType(
            Collections.singletonList(
                new RowField("f1", new ArrayType(new RowType(Collections.singletonList(
                    new RowField("f01", new IntType())
                ))))
            ));

        Integer value = 1;
        Row nestedRow = Row.of(value);
        Row[] testArrayOfRows = new Row[] {nestedRow};

        // We need this casting to an Object because Row.of(...) accepts varargs and without the
        // cast, during the runtime an array is interpreted as varargs and not single object
        // making the test fail on rowToRowData(...) before starting Flink Job.
        // The issue is also reported by Intellij code hints:
        // "Confusing argument '(testArrayOfArrays)', unclear if a varargs or non-varargs call is
        // desired. Cast to Object"
        Row testRow = Row.of((Object) testArrayOfRows);

        // WHEN
        RuntimeException exception = assertThrows(
            RuntimeException.class,
            () -> runFlinkJob(rowType, rowToRowData(rowType, testRow))
        );

        // THEN
        System.out.println(exception.getCause().getCause().getCause().getMessage());

        assertThat(
            exception.getCause().getCause().getCause().getMessage(),
            IsEqual.equalTo(
                "org.apache.parquet.io.ParquetEncodingException: empty fields are illegal,"
                    + " the field should be ommited completely instead")
        );
    }

    /**
     * This test tries to write a Parquet file with schema {@code ROW<Array<Map<String, Int>>>} This
     * is expected to fail due to issue in flink-parquet library, were writing complex nested types
     * is still not implemented fully.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testNestedComplexTypes_ArrayOfMap() {

        // GIVEN
        RowType rowType = new RowType(
            Collections.singletonList(
                new RowField(
                    "f1",
                    new ArrayType(new MapType(new VarCharType(), new IntType()))
                )
            ));

        Integer value = 1;
        Map<String, Integer> testMap = new HashMap<String, Integer>() {{
                put(String.valueOf(value), value);
            }};

        Map<String, Integer>[] testArrayOfMaps = new Map[] {testMap};

        // We need this casting to an Object because Row.of(...) accepts varargs and without the
        // cast, during the runtime an array is interpreted as varargs and not single object
        // making the test fail on rowToRowData(...) before starting Flink Job.
        // The issue is also reported by Intellij code hints:
        // "Confusing argument '(testArrayOfArrays)', unclear if a varargs or non-varargs call is
        // desired. Cast to Object"
        Row testRow = Row.of((Object) testArrayOfMaps);

        // WHEN
        RuntimeException exception = assertThrows(
            RuntimeException.class,
            () -> runFlinkJob(rowType, rowToRowData(rowType, testRow))
        );

        // THEN
        System.out.println(exception.getCause().getCause().getCause().getMessage());

        assertThat(
            exception.getCause().getCause().getCause().getMessage(),
            IsEqual.equalTo(
                "org.apache.parquet.io.ParquetEncodingException: empty fields are illegal,"
                    + " the field should be ommited completely instead")
        );
    }

    /**
     * In this method we check in short time intervals for the total time of 10 seconds whether
     * the DeltaLog for the table has been already created by the Flink job running in the deamon
     * thread
     *
     * @param deltaLog {@link DeltaLog} instance for test table
     * @throws InterruptedException when the thread is interrupted when waiting for the log to be
     *                              created
     */
    private void waitUntilDeltaLogExists(DeltaLog deltaLog) throws InterruptedException {
        int i = 0;
        while (deltaLog.snapshot().getVersion() < 0) {
            if (i > 20) throw new RuntimeException(
                "Timeout. DeltaLog for table has not been initialized");
            i++;
            Thread.sleep(500);
            deltaLog.update();
        }
    }

    /**
     * Runs Flink job in a daemon thread.
     * <p>
     * This workaround is needed because if we try to first run the Flink job and then query the
     * table with Delta Standalone Reader (DSR) then we are hitting "closed classloader exception"
     * which in short means that finished Flink job closes the classloader for the classes that DSR
     * tries to reuse.
     *
     * @param rowType  structure of the events in the streaming job
     * @param testData collection of test {@link RowData}
     */
    private void runFlinkJobInBackground(RowType rowType,
                                         List<RowData> testData) {
        new Thread(() -> runFlinkJob(rowType, testData)).start();
    }

    private void runFlinkJob(RowType rowType,
                             List<RowData> testData) {
        StreamExecutionEnvironment env = getTestStreamEnv();
        DeltaSink<RowData> deltaSink = DeltaSink
            .forRowData(
                new Path(deltaTablePath),
                DeltaTestUtils.getHadoopConf(), rowType).build();
        env.fromCollection(testData).sinkTo(deltaSink);
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static StreamExecutionEnvironment getTestStreamEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
        return env;
    }

    @SuppressWarnings("unchecked")
    private static List<RowData> rowToRowData(RowType rowType,
                                              Row row) {
        DataFormatConverters.DataFormatConverter<RowData, Row> CONVERTER =
            DataFormatConverters.getConverterForDataType(
                TypeConversions.fromLogicalToDataType(rowType));
        RowData rowData = CONVERTER.toInternal(row);
        return Collections.singletonList(rowData);
    }

    /**
     * Method that reads record written to a Delta table and with the use of Delta Standalone Reader
     * validates whether the read fields are equal to their original values.
     *
     * @param snapshot    current snapshot representing the table's state after the record has been
     *                    written by the Flink job
     * @param originalRow original row containing values before writing
     */
    public static void validate(Snapshot snapshot, Row originalRow) throws IOException {

        assertTrue(snapshot.getVersion() >= 0);
        assertTrue(snapshot.getAllFiles().size() > 0);

        Integer originalValue = (Integer) originalRow.getField(1);

        try (CloseableIterator<RowRecord> iterator = snapshot.open()) {

            RowRecord row;
            int numRows = 0;
            while (iterator.hasNext()) {
                row = iterator.next();
                numRows++;
                assertEquals(originalValue.floatValue(), row.getFloat("f1"), 0.0);
                assertEquals(originalValue.intValue(), row.getInt("f2"));
                assertEquals(originalValue.toString(), row.getString("f3"));
                assertEquals(originalValue.doubleValue(), row.getDouble("f4"), 0.0);
                assertFalse(row.getBoolean("f5"));
                assertEquals(originalValue.byteValue(), row.getByte("f6"));
                assertEquals(originalValue.shortValue(), row.getShort("f7"));
                assertEquals(originalValue.longValue(), row.getLong("f8"));
                assertEquals(
                    originalValue,
                    Integer.valueOf(new String(row.getBinary("f9"), StandardCharsets.UTF_8)));
                assertEquals(
                    originalValue,
                    Integer.valueOf(new String(row.getBinary("f10"), StandardCharsets.UTF_8)));
                assertEquals(
                    originalRow.getField(10), row.getTimestamp("f11").toLocalDateTime());
                assertEquals(originalRow.getField(11),
                    row.getTimestamp("f12").toLocalDateTime().toInstant(ZoneOffset.UTC));
                assertEquals(originalRow.getField(12), row.getDate("f13").toLocalDate());
                assertEquals(String.valueOf(originalValue), row.getString("f14"));
                BigDecimal expectedBigDecimal1 = BigDecimal.valueOf(originalValue);
                assertEquals(
                    expectedBigDecimal1,
                    row.getBigDecimal("f15").setScale(expectedBigDecimal1.scale()));
                BigDecimal expectedBigDecimal2 = new BigDecimal("11.11");
                assertEquals(
                    expectedBigDecimal2,
                    row.getBigDecimal("f16").setScale(expectedBigDecimal2.scale()));
            }
            assertEquals(1, numRows);
        }
    }

    /**
     * Method that reads record with nested types written to a Delta table and with the use of Delta
     * Standalone Reader validates whether the read fields are equal to their original values.
     *
     * @param snapshot    current snapshot representing the table's state after the record has been
     *                    written by the Flink job
     * @param originalRow original row containing values before writing
     */
    @SuppressWarnings("unchecked")
    private void validateNestedData(Snapshot snapshot, Row originalRow) throws IOException {

        assertTrue(snapshot.getVersion() >= 0);
        assertTrue(snapshot.getAllFiles().size() > 0);

        RowRecord row;
        int numRows = 0;
        try (CloseableIterator<RowRecord> iterator = snapshot.open()) {
            row = iterator.next();
            numRows++;

            Map<String, Integer> actualMap = row.getMap("f1");
            Map<String, Integer> expectedMap = (Map<String, Integer>) originalRow.getField(0);

            assertThat(actualMap, equalTo(expectedMap));
            assertThat(actualMap.get("1"), equalTo(expectedMap.get("1")));

            List<Integer> actualArray = row.getList("f2");
            Integer[] expectedArray = (Integer[]) originalRow.getField(1);
            assertThat(actualArray.toArray(new Integer[0]), equalTo(expectedArray));

            RowRecord actualRecord = row.getRecord("f3");
            Row expectedRecord = (Row) originalRow.getField(2);

            assertThat(actualRecord.getInt("f01"), equalTo(expectedRecord.getField(0)));
        }

        assertEquals(1, numRows);

    }

}
