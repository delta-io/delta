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
package io.delta.kernel.defaults.internal.parquet;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static io.delta.golden.GoldenTableUtils.goldenTableFile;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.delta.kernel.data.*;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;
import io.delta.kernel.utils.VectorUtils;

import io.delta.kernel.defaults.utils.DefaultKernelTestUtils;
import io.delta.kernel.defaults.internal.DefaultKernelUtils;

public class TestParquetBatchReader {
    /**
     * Test reads data from a Parquet file with data of various combinations of data types supported
     * by the Delta Lake table protocol.
     */
    private static final String ALL_TYPES_FILE =
        Arrays.stream(goldenTableFile("parquet-all-types").listFiles())
            .filter(file -> file.getName().endsWith(".parquet"))
            .map(File::getAbsolutePath)
            .findFirst()
            .get();

    private static final StructType ALL_TYPES_FILE_SCHEMA = new StructType()
        .add("byteType", ByteType.BYTE)
        .add("shortType", ShortType.SHORT)
        .add("integerType", IntegerType.INTEGER)
        .add("longType", LongType.LONG)
        .add("floatType", FloatType.FLOAT)
        .add("doubleType", DoubleType.DOUBLE)
        .add("decimal", new DecimalType(10, 2))
        .add("booleanType", BooleanType.BOOLEAN)
        .add("stringType", StringType.STRING)
        .add("binaryType", BinaryType.BINARY)
        .add("dateType", DateType.DATE)
        .add("timestampType", TimestampType.TIMESTAMP)
        .add("nested_struct",
            new StructType()
                .add("aa", StringType.STRING)
                .add("ac", new StructType().add("aca", IntegerType.INTEGER)))
        .add("array_of_prims",
            new ArrayType(IntegerType.INTEGER, true))
        .add("array_of_arrays",
            new ArrayType(new ArrayType(IntegerType.INTEGER, true), true))
        .add("array_of_structs",
            new ArrayType(new StructType().add("ab", LongType.LONG), true))
        .add("map_of_prims", new MapType(IntegerType.INTEGER, LongType.LONG, true))
        .add("map_of_rows", new MapType(
            IntegerType.INTEGER,
            new StructType().add("ab", LongType.LONG),
            true))
        .add("map_of_arrays", new MapType(
            LongType.LONG,
            new ArrayType(IntegerType.INTEGER, true),
            true));

    @Test
    public void readAllTypesOfData()
        throws Exception {
        readAndVerify(ALL_TYPES_FILE_SCHEMA, 90 /* readBatchSize */);
    }

    @Test
    public void readSubsetOfColumns()
        throws Exception {
        StructType readSchema = new StructType()
            .add("byteType", ByteType.BYTE)
            .add("booleanType", BooleanType.BOOLEAN)
            .add("stringType", StringType.STRING)
            .add("dateType", DateType.DATE)
            .add("nested_struct",
                new StructType()
                    .add("aa", StringType.STRING)
                    .add("ac", new StructType().add("aca", IntegerType.INTEGER)))
            .add("array_of_prims",
                new ArrayType(IntegerType.INTEGER, true));

        readAndVerify(readSchema, 73 /* readBatchSize */);
    }

    @Test
    public void readSubsetOfColumnsWithMissingColumnsInFile()
        throws Exception {
        StructType readSchema = new StructType()
            .add("booleanType", BooleanType.BOOLEAN)
            .add("integerType", IntegerType.INTEGER)
            .add("missing_column_struct",
                new StructType().add("ab", IntegerType.INTEGER))
            .add("longType", LongType.LONG)
            .add("missing_column_primitive", DateType.DATE)
            .add("nested_struct",
                new StructType()
                    .add("aa", StringType.STRING)
                    .add("ac", new StructType().add("aca", IntegerType.INTEGER))
            );

        readAndVerify(readSchema, 23 /* readBatchSize */);
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
            .add("id", LongType.LONG)
            .add(StructField.METADATA_ROW_INDEX_COLUMN);

        Configuration conf = new Configuration();
        // Set the batch size small enough so there will be multiple batches
        conf.setInt("delta.kernel.default.parquet.reader.batch-size", 2);
        ParquetBatchReader reader = new ParquetBatchReader(conf);

        for (String filePath : parquetFiles) {
            try (CloseableIterator<ColumnarBatch> iter = reader.read(filePath, readSchema)) {
                while (iter.hasNext()) {
                    ColumnarBatch batch = iter.next();
                    for (int i = 0; i < batch.getSize(); i++) {
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
                for (int i = 0; i < batch.getSize(); i++) {
                    long id = batch.getColumnVector(0).getLong(i);
                    long rowIndex = batch.getColumnVector(1).getLong(i);
                    assertEquals(id, rowIndex);
                }
            }
        }
    }

    private static Configuration newConf(Optional<Integer> batchSize) {
        Configuration conf = new Configuration();
        if (batchSize.isPresent()) {
            conf.set("delta.kernel.default.parquet.reader.batch-size", batchSize.get().toString());
        }
        return conf;
    }

    private static void readAndVerify(StructType readSchema, int readBatchSize)
        throws Exception {
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
        StructType readSchema) throws Exception {
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
        int rowId) {
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
                    } else {
                        assertEquals(expValue.booleanValue(), vector.getBoolean(batchWithIdx._2));
                    }
                    break;
                }
                case "bytetype": {
                    Byte expValue = (rowId % 72 != 0) ? (byte) rowId : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    } else {
                        assertEquals(expValue.byteValue(), vector.getByte(batchWithIdx._2));
                    }
                    break;
                }
                case "shorttype": {
                    Short expValue = (rowId % 56 != 0) ? (short) rowId : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    } else {
                        assertEquals(expValue.shortValue(), vector.getShort(batchWithIdx._2));
                    }
                    break;
                }
                case "datetype": {
                    Integer expValue = (rowId % 61 != 0) ?
                        (int) Math.floorDiv(
                            rowId * 20000000L,
                            DefaultKernelUtils.DateTimeConstants.MILLIS_PER_DAY) : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    } else {
                        assertEquals(expValue.intValue(), vector.getInt(batchWithIdx._2));
                    }
                    break;
                }
                case "integertype": {
                    Integer expValue = (rowId % 23 != 0) ? rowId : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    } else {
                        assertEquals(expValue.intValue(), vector.getInt(batchWithIdx._2));
                    }
                    break;
                }
                case "longtype": {
                    Long expValue = (rowId % 25 != 0) ? rowId + 1L : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    } else {
                        assertEquals(expValue.longValue(), vector.getLong(batchWithIdx._2));
                    }
                    break;
                }
                case "floattype": {
                    Float expValue = (rowId % 28 != 0) ? (rowId * 0.234f) : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    } else {
                        assertEquals(expValue.floatValue(), vector.getFloat(batchWithIdx._2), 0.02);
                    }
                    break;
                }
                case "doubletype": {
                    Double expValue = (rowId % 54 != 0) ? (rowId * 234234.23d) : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    } else {
                        assertEquals(expValue.doubleValue(), vector.getDouble(batchWithIdx._2),
                            0.02);
                    }
                    break;
                }
                case "stringtype": {
                    String expValue = (rowId % 57 != 0) ? Integer.toString(rowId) : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    } else {
                        assertEquals(expValue, vector.getString(batchWithIdx._2));
                    }
                    break;
                }
                case "binarytype": {
                    byte[] expValue = (rowId % 59 != 0) ? Integer.toString(rowId).getBytes() : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    } else {
                        assertArrayEquals(expValue, vector.getBinary(batchWithIdx._2));
                    }
                    break;
                }
                case "timestamptype": {
                    // Tests only for spark.sql.parquet.outputTimestampTyp = INT96, other formats
                    // are tested in end-to-end tests in DeltaTableReadsSuite
                    Long expValue = (rowId % 62 != 0) ? 23423523L * rowId * 1000 : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    } else {
                        assertEquals(expValue.longValue(), vector.getLong(batchWithIdx._2));
                    }
                    break;
                }
                case "decimal": {
                    BigDecimal expValue = (rowId % 67 != 0) ?
                        // Value is rounded to scale=2 when written
                        new BigDecimal(rowId * 123.52).setScale(2, RoundingMode.HALF_UP) : null;
                    if (expValue == null) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                    } else {
                        assertEquals(expValue, vector.getDecimal(batchWithIdx._2));
                    }
                    break;
                }
                case "nested_struct":
                    validateNestedStructColumn(vector, batchWithIdx._2, rowId);
                    break;
                case "array_of_prims": {
                    boolean expIsNull = rowId % 25 == 0;
                    if (expIsNull) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                        assertNull(vector.getArray(batchWithIdx._2));
                    } else if (rowId % 29 == 0) {
                        checkArrayValue(vector.getArray(batchWithIdx._2), IntegerType.INTEGER,
                                Collections.<Integer>emptyList());
                    } else {
                        List<Integer> expArray = Arrays.asList(rowId, null, rowId + 1);
                        checkArrayValue(vector.getArray(batchWithIdx._2), IntegerType.INTEGER,
                                expArray);
                    }
                    break;
                }
                case "array_of_arrays":
                    validateArrayOfArraysColumn(vector, batchWithIdx._2, rowId);
                    break;
                case "array_of_structs": {
                    assertFalse(vector.isNullAt(batchWithIdx._2));
                    ArrayValue arrayValue = vector.getArray(batchWithIdx._2);
                    ColumnVector elementVector = arrayValue.getElements();
                    assertEquals(2, arrayValue.getSize());
                    assertEquals(2, elementVector.getSize());
                    assertTrue(elementVector.getDataType() instanceof StructType);
                    assertEquals(rowId, elementVector.getChild(0).getLong(0));
                    assertTrue(elementVector.isNullAt(1));
                    break;
                }
                case "map_of_prims": {
                    boolean expIsNull = rowId % 28 == 0;
                    if (expIsNull) {
                        assertTrue(vector.isNullAt(batchWithIdx._2));
                        assertNull(vector.getMap(batchWithIdx._2));
                    } else if (rowId % 30 == 0) {
                        checkMapValue(
                                vector.getMap(batchWithIdx._2),
                                IntegerType.INTEGER,
                                LongType.LONG,
                                Collections.<Integer, Long>emptyMap()
                        );
                    } else {
                        Map<Integer, Long> expValue = new HashMap<Integer, Long>() {
                            {
                                put(rowId, (rowId % 29 == 0) ? null : (rowId + 2L));
                                put((rowId % 27 != 0) ? (rowId + 2) : (rowId + 3), rowId + 9L);

                            }
                        };
                        checkMapValue(
                                vector.getMap(batchWithIdx._2),
                                IntegerType.INTEGER,
                                LongType.LONG,
                                expValue
                        );
                    }
                    break;
                }
                case "map_of_rows": {
                    // Map(i + 1 -> (if (i % 10 == 0) Row((i*20).longValue()) else null))
                    assertFalse(vector.isNullAt(batchWithIdx._2));
                    MapValue mapValue = vector.getMap(batchWithIdx._2);
                    Map<Integer, Row> actValue = VectorUtils.toJavaMap(mapValue);

                    // entry 0: key = rowId
                    Integer key0 = rowId + 1;
                    boolean expValue0IsNull = rowId % 10 != 0;
                    Row actValue0 = actValue.get(key0);
                    if (expValue0IsNull) {
                        assertNull(actValue0);
                    } else {
                        Long actValue0Member = actValue0.getLong(0);
                        Long expValue0Member = rowId * 20L;
                        assertEquals(expValue0Member, actValue0Member);
                    }
                    break;
                }
                case "map_of_arrays":
                    validateMapOfArraysColumn(vector, batchWithIdx._2, rowId);
                    break;
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

    private static void validateNestedStructColumn(
        ColumnVector vector, int batchRowId, int tableRowId) {
        boolean expNull = tableRowId % 63 == 0;
        if (expNull) {
            assertTrue(vector.isNullAt(batchRowId));
            return;
        }

        boolean expAaValNull = tableRowId % 19 == 0;
        boolean expAcValNull = tableRowId % 19 == 0 || tableRowId % 23 == 0;
        final int aaColOrdinal = 0;
        final int acColOrdinal = 1;

        assertEquals(vector.getChild(aaColOrdinal).isNullAt(batchRowId), expAaValNull);
        assertEquals(vector.getChild(acColOrdinal).isNullAt(batchRowId), expAcValNull);

        if (!expAaValNull) {
            String aaVal = vector.getChild(aaColOrdinal).getString(batchRowId);
            assertEquals(Integer.toString(tableRowId), aaVal);
        }
        if (!expAcValNull) {
            int actAcaVal = vector.getChild(acColOrdinal).getChild(0).getInt(batchRowId);
            assertEquals(tableRowId, actAcaVal);
        }
    }

    private static void validateArrayOfArraysColumn(
        ColumnVector vector, int batchRowId, int tableRowId) {
        boolean expIsNull = tableRowId % 8 == 0;
        if (expIsNull) {
            assertTrue(vector.isNullAt(batchRowId));
            return;
        }

        List<Integer> singleElemArray = Arrays.asList(tableRowId);
        List<Integer> doubleElemArray = Arrays.asList(tableRowId + 10, tableRowId + 20);
        List<Integer> arrayWithNulls = Arrays.asList(null, tableRowId + 200);
        List<Integer> singleElemNullArray = Collections.singletonList(null);
        List<Integer> emptyArray = Collections.emptyList();

        List<List<Integer>> expArray = null;
        switch (tableRowId % 7) {
            case 0:
                expArray = Arrays.asList(singleElemArray, singleElemArray, arrayWithNulls);
                break;
            case 1:
                expArray = Arrays.asList(singleElemArray, doubleElemArray, emptyArray);
                break;
            case 2:
                expArray = Arrays.asList(arrayWithNulls);
                break;
            case 3:
                expArray = Arrays.asList(singleElemNullArray);
                break;
            case 4:
                expArray = Collections.singletonList(null);
                break;
            case 5:
                expArray = Collections.singletonList(emptyArray);
                break;
            case 6:
                expArray = Collections.emptyList();
                break;
        }
        DataType expDataType = new ArrayType(IntegerType.INTEGER, true);
        checkArrayValue(vector.getArray(batchRowId), expDataType, expArray);
    }

    private static void validateMapOfArraysColumn(
        ColumnVector vector, int batchRowId, int tableRowId) {
        boolean expIsNull = tableRowId % 30 == 0;
        if (expIsNull) {
            assertTrue(vector.isNullAt(batchRowId));
            return;
        }

        final List<Integer> val1;
        if (tableRowId % 4 == 0) {
            val1 = Arrays.asList(tableRowId, null, tableRowId + 1);
        } else {
            val1 = Collections.emptyList();
        }
        final List<Integer> val2;
        if (tableRowId % 7 == 0) {
            val2 = Collections.emptyList();
        } else {
            val2 = Collections.singletonList(null);
        }

        Map<Long, List<Integer>> expMap = Collections.emptyMap();
        if (tableRowId % 24 != 0) {
            expMap = new HashMap<Long, List<Integer>>() {
                {
                    put((long) tableRowId, val1);
                    put(tableRowId + 1L, val2);
                }
            };
        }
        checkMapValue(
                vector.getMap(batchRowId),
                LongType.LONG,
                new ArrayType(IntegerType.INTEGER, true),
                expMap
        );
    }

    private static Tuple2<ColumnarBatch, Integer> getBatchForRowId(
        List<ColumnarBatch> batches, int rowId) {
        int indexStart = 0;
        for (ColumnarBatch batch : batches) {
            if (indexStart <= rowId && rowId < indexStart + batch.getSize()) {
                return new Tuple2<>(batch, rowId - indexStart);
            }
            indexStart += batch.getSize();
        }

        throw new IllegalArgumentException("row id is not found: " + rowId);
    }

    private static <T> void checkArrayValue(
            ArrayValue arrayValue, DataType expDataType, List<T> expList) {
        int size = expList.size();
        ColumnVector elementVector = arrayValue.getElements();
        // Check the size is as expected and arrayValue.getSize == elementVector.getSize
        assertEquals(size, arrayValue.getSize());
        assertEquals(size, elementVector.getSize());
        // Check the element vector has the correct data type
        assertEquals(elementVector.getDataType(), expDataType);
        // Check the elements are correct
        assertEquals(expList, VectorUtils.toJavaList(arrayValue));
        assertThrows(IllegalArgumentException.class,
                () -> DefaultKernelTestUtils.getValueAsObject(elementVector, size + 1));
    }

    private static <K, V> void checkMapValue(
            MapValue mapValue, DataType keyDataType, DataType valueDataType, Map<K, V> expMap) {
        int size = expMap.size();
        ColumnVector keyVector = mapValue.getKeys();
        ColumnVector valueVector = mapValue.getValues();
        // Check the size mapValue.getSize == keyVector.getSize == valueVector.getSize
        assertEquals(size, mapValue.getSize());
        assertEquals(size, keyVector.getSize());
        assertEquals(size, valueVector.getSize());
        // Check the key and value vector has the correct data type
        assertEquals(keyVector.getDataType(), keyDataType);
        assertEquals(valueVector.getDataType(), valueDataType);
        // Check the elements are correct
        assertEquals(expMap, VectorUtils.toJavaMap(mapValue));
        assertThrows(IllegalArgumentException.class,
                () -> DefaultKernelTestUtils.getValueAsObject(keyVector, size + 1));
        assertThrows(IllegalArgumentException.class,
                () -> DefaultKernelTestUtils.getValueAsObject(valueVector, size + 1));
    }
}
