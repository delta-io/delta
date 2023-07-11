/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.parquet;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.sql.Date;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DefaultKernelTestUtils;
import io.delta.kernel.utils.Tuple2;

public class TestParquetBatchReader
{
    /**
     * Test reads data from a Parquet file with data of various combinations of data types supported
     * byt Delta Lake table protocol. Code for generating the golden parquet files is located:
     * https://gist.github.com/vkorukanti/238bad726545e466202278966989f02b (TODO: Move this a better
     * place).
     */
    private static final String ALL_TYPES_FILE =
        DefaultKernelTestUtils.getTestResourceFilePath("parquet/all_types.parquet");

    private static final StructType ALL_TYPES_FILE_SCHEMA = new StructType()
        .add("byteType", ByteType.INSTANCE)
        .add("shortType", ShortType.INSTANCE)
        .add("integerType", IntegerType.INSTANCE)
        .add("longType", LongType.INSTANCE)
        .add("floatType", FloatType.INSTANCE)
        .add("doubleType", DoubleType.INSTANCE)
        // .add("decimal", new DecimalType(10, 2)) // TODO
        .add("booleanType", BooleanType.INSTANCE)
        .add("stringType", StringType.INSTANCE)
        .add("binaryType", BinaryType.INSTANCE)
        .add("dateType", DateType.INSTANCE)
        // .add("timestampType", TimestampType.INSTANCE) // TODO
        .add("nested_struct",
            new StructType()
                .add("aa", StringType.INSTANCE)
                .add("ac", new StructType().add("aca", IntegerType.INSTANCE)))
        .add("array_of_prims",
            new ArrayType(IntegerType.INSTANCE, true))
        .add("array_of_structs",
            new ArrayType(new StructType().add("ab", LongType.INSTANCE), true))
        .add("map_of_prims", new MapType(IntegerType.INSTANCE, LongType.INSTANCE, true))
        .add("map_of_complex", new MapType(
            IntegerType.INSTANCE,
            new StructType().add("ab", LongType.INSTANCE),
            true));

    private static final LocalDate EPOCH = new Date(0).toLocalDate().ofEpochDay(0);

    @Test
    public void readAllTypesOfData()
        throws Exception
    {
        readAndVerify(ALL_TYPES_FILE_SCHEMA, 90 /* readBatchSize */);
    }

    @Test
    public void readSubsetOfColumns()
        throws Exception
    {
        StructType readSchema = new StructType()
            .add("byteType", ByteType.INSTANCE)
            .add("booleanType", BooleanType.INSTANCE)
            .add("stringType", StringType.INSTANCE)
            .add("dateType", DateType.INSTANCE)
            .add("nested_struct",
                new StructType()
                    .add("aa", StringType.INSTANCE)
                    .add("ac", new StructType().add("aca", IntegerType.INSTANCE))
            ).add("array_of_prims",
                new ArrayType(IntegerType.INSTANCE, true)
            );

        readAndVerify(readSchema, 73 /* readBatchSize */);
    }

    @Test
    public void readSubsetOfColumnsWithMissingColumnsInFile()
        throws Exception
    {
        StructType readSchema = new StructType()
            .add("booleanType", BooleanType.INSTANCE)
            .add("integerType", IntegerType.INSTANCE)
            .add("missing_column_struct",
                new StructType().add("ab", IntegerType.INSTANCE))
            .add("longType", LongType.INSTANCE)
            .add("missing_column_primitive", DateType.INSTANCE)
            .add("nested_struct",
                new StructType()
                    .add("aa", StringType.INSTANCE)
                    .add("ac", new StructType().add("aca", IntegerType.INSTANCE))
            );

        readAndVerify(readSchema, 23 /* readBatchSize */);
    }

    private static Configuration newConf(Optional<Integer> batchSize)
    {
        Configuration conf = new Configuration();
        if (batchSize.isPresent()) {
            conf.set("delta.kernel.default.parquet.reader.batch-size", batchSize.get().toString());
        }
        return conf;
    }

    private static void readAndVerify(StructType readSchema, int readBatchSize)
        throws Exception
    {
        ParquetBatchReader batchReader =
            new ParquetBatchReader(newConf(Optional.of(readBatchSize)));
        List<ColumnarBatch> batches =
            readAsBatches(batchReader, ALL_TYPES_FILE, readSchema);

        for (int rowId = 0; rowId < 200; rowId++) {
            verifyRowFromAllTypesFile(readSchema, batches, rowId);
        }
    }

    private static List<ColumnarBatch> readAsBatches(
        ParquetBatchReader parquetReader,
        String path,
        StructType readSchema) throws Exception
    {
        List<ColumnarBatch> batches = new ArrayList<>();
        try (CloseableIterator<ColumnarBatch> dataIter = parquetReader.read(path, readSchema)) {
            while (dataIter.hasNext()) {
                batches.add(dataIter.next());
            }
        }
        return batches;
    }

    private static void verifyRowFromAllTypesFile(
        StructType readSchema,
        List<ColumnarBatch> batches,
        int rowId)
    {
        Tuple2<ColumnarBatch, Integer> batchWithIdx = getBatchForRowId(batches, rowId);
        int ordinal = 0;
        for (StructField structField : readSchema.fields()) {
            String name = structField.getName().toLowerCase();
            ColumnVector vector = batchWithIdx._1.getColumnVector(ordinal);
            switch (name) {
                case "booleantype": {
                    Boolean expValue = (rowId % 87 != 0) ? rowId % 2 == 0 : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    }
                    else {
                        assertEquals(expValue.booleanValue(), vector.getBoolean(batchWithIdx._2));
                    }
                    break;
                }
                case "bytetype": {
                    Byte expValue = (rowId % 72 != 0) ? (byte) rowId : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    }
                    else {
                        assertEquals(expValue.byteValue(), vector.getByte(batchWithIdx._2));
                    }
                    break;
                }
                case "shorttype": {
                    Short expValue = (rowId % 56 != 0) ? (short) rowId : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    }
                    else {
                        assertEquals(expValue.shortValue(), vector.getShort(batchWithIdx._2));
                    }
                    break;
                }
                case "datetype": {
                    // Set `-Duser.timezone="UTC"` as JVM arg to pass this test in computers
                    // whose local timezone is non-UTC zone.
                    LocalDate expValue = (rowId % 61 != 0) ?
                        new Date(rowId * 20000000L).toLocalDate() : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    }
                    else {
                        long numDaysSinceEpoch = ChronoUnit.DAYS.between(EPOCH, expValue);
                        assertEquals(numDaysSinceEpoch, vector.getInt(batchWithIdx._2));
                    }
                    break;
                }
                case "integertype": {
                    Integer expValue = (rowId % 23 != 0) ? rowId : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    }
                    else {
                        assertEquals(expValue.intValue(), vector.getInt(batchWithIdx._2));
                    }
                    break;
                }
                case "longtype": {
                    Long expValue = (rowId % 25 != 0) ? rowId + 1L : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    }
                    else {
                        assertEquals(expValue.longValue(), vector.getLong(batchWithIdx._2));
                    }
                    break;
                }
                case "floattype": {
                    Float expValue = (rowId % 28 != 0) ? (rowId * 0.234f) : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    }
                    else {
                        assertEquals(expValue.floatValue(), vector.getFloat(batchWithIdx._2), 0.02);
                    }
                    break;
                }
                case "doubletype": {
                    Double expValue = (rowId % 54 != 0) ? (rowId * 234234.23d) : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    }
                    else {
                        assertEquals(expValue.doubleValue(), vector.getDouble(batchWithIdx._2),
                            0.02);
                    }
                    break;
                }
                case "stringtype": {
                    String expValue = (rowId % 57 != 0) ? Integer.toString(rowId) : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    }
                    else {
                        assertEquals(expValue, vector.getString(batchWithIdx._2));
                    }
                    break;
                }
                case "binarytype": {
                    byte[] expValue = (rowId % 59 != 0) ? Integer.toString(rowId).getBytes() : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    }
                    else {
                        assertArrayEquals(expValue, vector.getBinary(batchWithIdx._2));
                    }
                    break;
                }
                case "timestamptype": {
                    throw new UnsupportedOperationException("not yet implemented: " + name);
                }
                case "decimal": {
                    throw new UnsupportedOperationException("not yet implemented: " + name);
                }
                case "nested_struct": {
                    Row struct = vector.getStruct(batchWithIdx._2);
                    assertFalse(vector.isNullAt(batchWithIdx._2));
                    String aaVal = struct.getString(0);
                    assertEquals(Integer.toString(rowId), aaVal);

                    boolean expAcValNull = rowId % 23 == 0;
                    Row acVal = struct.getStruct(1);
                    if (expAcValNull) {
                        assertTrue(struct.isNullAt(1));
                        assertNull(acVal);
                    }
                    else {
                        int actAcaVal = acVal.getInt(0);
                        assertEquals(rowId, actAcaVal);
                    }
                    break;
                }
                case "array_of_prims": {
                    boolean expIsNull = rowId % 25 == 0;
                    if (expIsNull) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    }
                    else if (rowId % 29 == 0) {
                        // TODO: Parquet group converters calls to start/end don't differentiate
                        // between empty array or null array. The current reader always treats both
                        // of them nulls.
                        // assertEquals(Collections.emptyList(), vector.getArray(batchWithIdx._2));
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    }
                    else {
                        List<Integer> expArray = Arrays.asList(rowId, null, rowId + 1);
                        List<Integer> actArray = vector.getArray(batchWithIdx._2);
                        assertEquals(expArray, actArray);
                    }
                    break;
                }
                case "array_of_structs": {
                    assertFalse(vector.isNullAt(batchWithIdx._2));
                    List<Row> actArray = vector.getArray(batchWithIdx._2);
                    assertTrue(actArray.size() == 2);
                    Row item0 = actArray.get(0);
                    assertEquals(rowId, item0.getLong(0));
                    assertNull(actArray.get(1));
                    break;
                }
                case "map_of_prims": {
                    boolean expIsNull = rowId % 28 == 0;
                    if (expIsNull) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    }
                    else if (rowId % 30 == 0) {
                        // TODO: Parquet group converters calls to start/end don't differentiate
                        // between empty map or null map. The current reader always treats both
                        // of them nulls.
                        // assertEquals(Collections.emptyList(), vector.getMap(batchWithIdx._2));
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    }
                    else {
                        Map<Integer, Long> actValue = vector.getMap(batchWithIdx._2);
                        assertTrue(actValue.size() == 2);

                        // entry 0: key = rowId
                        Integer key0 = rowId;
                        Long actValue0 = actValue.get(key0);
                        Long expValue0 = (rowId % 29 == 0) ? null : (rowId + 2L);
                        assertEquals(expValue0, actValue0);

                        // entry 1: key = if (rowId % 27 != 0) rowId + 2 else null
                        // TODO: Not sure if this is a bug or expected behavior. In Delta-Spark,
                        // whenever the map key value is null - it is stored as 0. Not sure
                        // what happens for non-integer keys.
                        // Integer key1 = (rowId % 27 == 0) ? null : rowId + 2;
                        Integer key1 = (rowId % 27 == 0) ? 0 : rowId + 2;
                        Long actValue1 = actValue.get(key1);
                        Long expValue1 = rowId + 9L;
                        assertEquals(expValue1, actValue1);
                    }
                    break;
                }
                case "map_of_complex": {
                    // Map(i + 1 -> (if (i % 10 == 0) Row((i*20).longValue()) else null))
                    assertFalse(vector.isNullAt(batchWithIdx._2));
                    Map<Integer, Row> actValue = vector.getMap(batchWithIdx._2);

                    // entry 0: key = rowId
                    Integer key0 = rowId + 1;
                    boolean expValue0IsNull = rowId % 10 != 0;
                    Row actValue0 = actValue.get(key0);
                    if (expValue0IsNull) {
                        assertNull(actValue0);
                    }
                    else {
                        Long actValue0Member = actValue0.getLong(0);
                        Long expValue0Member = rowId * 20L;
                        assertEquals(expValue0Member, actValue0Member);
                    }
                    break;
                }
                case "missing_column_primitive":
                case "missing_column_struct": {
                    assertTrue(vector.isNullAt(batchWithIdx._2));
                    break;
                }
                default:
                    throw new IllegalArgumentException("unknown column: " + name);
            }
            ordinal++;
        }
    }

    private static Tuple2<ColumnarBatch, Integer> getBatchForRowId(
        List<ColumnarBatch> batches, int rowId)
    {
        int indexStart = 0;
        for (ColumnarBatch batch : batches) {
            if (indexStart <= rowId && rowId < indexStart + batch.getSize()) {
                return new Tuple2<>(batch, rowId - indexStart);
            }
            indexStart += batch.getSize();
        }

        throw new IllegalArgumentException("row id is not found: " + rowId);
    }

    @Test
    public void requestRowIndices() throws IOException {
        String path = DefaultKernelTestUtils.getTestResourceFilePath("parquet-basic-row-indexes");
        File dir = new File(URI.create(path).getPath());
        List<String> parquetFiles = Arrays.stream(Objects.requireNonNull(dir.listFiles()))
                .filter(file -> file.getName().endsWith(".parquet"))
                .map(File::getAbsolutePath)
                .collect(Collectors.toList());

        StructType readSchema = new StructType()
                .add("id", LongType.INSTANCE)
                .add(StructField.ROW_INDEX_COLUMN);

        Configuration conf = new Configuration();
        // Set the batch size small enough so there will be multiple batches
        conf.setInt("delta.kernel.default.parquet.reader.batch-size", 2);
        ParquetBatchReader reader = new ParquetBatchReader(conf);

        for (String filePath : parquetFiles) {
            try (CloseableIterator<ColumnarBatch> iter = reader.read(filePath, readSchema)) {
                while (iter.hasNext()) {
                    ColumnarBatch batch = iter.next();
                    for (int i = 0; i < batch.getSize(); i ++) {
                        long id = batch.getColumnVector(0).getLong(i);
                        long rowIndex = batch.getColumnVector(1).getLong(i);
                        assertEquals(id % 10, rowIndex);
                    }
                }
            }
        }

        // File with multiple row-groups [0, 20000) where rowIndex = id
        String filePath = DefaultKernelTestUtils.getTestResourceFilePath(
                "parquet/row_index_multiple_row_groups.parquet");
        reader = new ParquetBatchReader(new Configuration());
        try (CloseableIterator<ColumnarBatch> iter = reader.read(filePath, readSchema)) {
            while (iter.hasNext()) {
                ColumnarBatch batch = iter.next();
                for (int i = 0; i < batch.getSize(); i ++) {
                    long id = batch.getColumnVector(0).getLong(i);
                    long rowIndex = batch.getColumnVector(1).getLong(i);
                    assertEquals(id, rowIndex);
                }
            }
        }
    }
}
