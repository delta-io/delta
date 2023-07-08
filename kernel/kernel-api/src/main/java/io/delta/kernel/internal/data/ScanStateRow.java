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
package io.delta.kernel.internal.data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;

/**
 * Expose the common scan state for all scan files.
 */
public class ScanStateRow
    implements Row
{
    private static final Map<Integer, Function<ScanStateRow, Object>>
        ordinalToAccessor = new HashMap<>();
    private static final Map<Integer, String> ordinalToColName = new HashMap<>();
    private static final StructType schema = new StructType()
        .add("configuration", new MapType(StringType.INSTANCE, StringType.INSTANCE, false))
        .add("logicalSchemaString", StringType.INSTANCE)
        .add("physicalSchemaString", StringType.INSTANCE)
        .add("partitionColumns", new ArrayType(StringType.INSTANCE, false))
        .add("minReaderVersion", IntegerType.INSTANCE)
        .add("minWriterVersion", IntegerType.INSTANCE)
        .add("tablePath", StringType.INSTANCE);

    static {
        ordinalToAccessor.put(0, (a) -> a.getConfiguration());
        ordinalToAccessor.put(1, (a) -> a.getReadSchemaLogicalJson());
        ordinalToAccessor.put(2, (a) -> a.getReadSchemaPhysicalJson());
        ordinalToAccessor.put(3, (a) -> a.getPartitionColumns());
        ordinalToAccessor.put(4, (a) -> a.getMinReaderVersion());
        ordinalToAccessor.put(5, (a) -> a.getMinWriterVersion());
        ordinalToAccessor.put(6, (a) -> a.getTablePath());

        ordinalToColName.put(0, "configuration");
        ordinalToColName.put(1, "logicalSchemaString");
        ordinalToColName.put(2, "physicalSchemaString");
        ordinalToColName.put(3, "partitionColumns");
        ordinalToColName.put(4, "minReaderVersion");
        ordinalToColName.put(5, "minWriterVersion");
        ordinalToColName.put(6, "tablePath");
    }

    private static final Map<String, Integer> colNameToOrdinal = ordinalToColName
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

    public static int getLogicalSchemaStringColOrdinal()
    {
        return getOrdinal("logicalSchemaString");
    }

    public static int getPhysicalSchemaStringColOrdinal()
    {
        return getOrdinal("physicalSchemaString");
    }

    public static int getPartitionColumnsColOrdinal()
    {
        return getOrdinal("partitionColumns");
    }

    public static int getConfigurationColOrdinal()
    {
        return getOrdinal("configuration");
    }

    public static String getTablePath(Row row) {
        return row.getString(getOrdinal("tablePath"));
    }

    private final Map<String, String> configuration;
    private final List<String> partitionColumns;
    private final int minReaderVersion;
    private final int minWriterVersion;
    private final String readSchemaLogicalJson;
    private final String readSchemaPhysicalJson;
    private String tablePath;

    public ScanStateRow(
        Metadata metadata,
        Protocol protocol,
        String readSchemaLogicalJson,
        String readSchemaPhysicalJson,
        String tablePath)
    {
        this.configuration = metadata.getConfiguration();
        this.partitionColumns = metadata.getPartitionColumns();
        this.minReaderVersion = protocol.getMinReaderVersion();
        this.minWriterVersion = protocol.getMinWriterVersion();
        this.readSchemaLogicalJson = readSchemaLogicalJson;
        this.readSchemaPhysicalJson = readSchemaPhysicalJson;
        this.tablePath = tablePath;
    }

    public Map<String, String> getConfiguration()
    {
        return configuration;
    }

    public List<String> getPartitionColumns()
    {
        return partitionColumns;
    }

    public int getMinReaderVersion()
    {
        return minReaderVersion;
    }

    public int getMinWriterVersion()
    {
        return minWriterVersion;
    }

    public String getReadSchemaPhysicalJson()
    {
        return readSchemaPhysicalJson;
    }

    public String getReadSchemaLogicalJson()
    {
        return readSchemaLogicalJson;
    }

    public String getTablePath()
    {
        return tablePath;
    }

    @Override
    public StructType getSchema()
    {
        return schema;
    }

    @Override
    public boolean isNullAt(int ordinal)
    {
        return getValue(ordinal) == null;
    }

    @Override
    public boolean getBoolean(int ordinal)
    {
        throwIfUnsafeAccess(ordinal, BooleanType.class, "boolean");
        return (boolean) getValue(ordinal);
    }

    @Override
    public byte getByte(int ordinal)
    {
        throwIfUnsafeAccess(ordinal, ByteType.class, "byte");
        return (byte) getValue(ordinal);
    }

    @Override
    public short getShort(int ordinal)
    {
        throwIfUnsafeAccess(ordinal, ShortType.class, "short");
        return (short) getValue(ordinal);
    }

    @Override
    public int getInt(int ordinal)
    {
        throwIfUnsafeAccess(ordinal, IntegerType.class, "integer");
        return (int) getValue(ordinal);
    }

    @Override
    public long getLong(int ordinal)
    {
        throwIfUnsafeAccess(ordinal, LongType.class, "long");
        return (long) getValue(ordinal);
    }

    @Override
    public float getFloat(int ordinal)
    {
        throwIfUnsafeAccess(ordinal, FloatType.class, "float");
        return (float) getValue(ordinal);
    }

    @Override
    public double getDouble(int ordinal)
    {
        throwIfUnsafeAccess(ordinal, DoubleType.class, "double");
        return (double) getValue(ordinal);
    }

    @Override
    public String getString(int ordinal)
    {
        throwIfUnsafeAccess(ordinal, StringType.class, "string");
        return (String) getValue(ordinal);
    }

    @Override
    public byte[] getBinary(int ordinal)
    {
        throwIfUnsafeAccess(ordinal, BinaryType.class, "binary");
        return (byte[]) getValue(ordinal);
    }

    @Override
    public Row getStruct(int ordinal)
    {
        throwIfUnsafeAccess(ordinal, StructType.class, "struct");
        return (Row) getValue(ordinal);
    }

    @Override
    public <T> List<T> getArray(int ordinal)
    {
        // TODO: not sufficient check, also need to check the element type
        throwIfUnsafeAccess(ordinal, ArrayType.class, "array");
        return (List<T>) getValue(ordinal);
    }

    @Override
    public <K, V> Map<K, V> getMap(int ordinal)
    {
        // TODO: not sufficient check, also need to check the element types
        throwIfUnsafeAccess(ordinal, MapType.class, "map");
        return (Map<K, V>) getValue(ordinal);
    }

    private Object getValue(int ordinal)
    {
        return ordinalToAccessor.get(ordinal).apply(this);
    }

    private DataType dataType(int ordinal)
    {
        if (schema.length() <= ordinal) {
            throw new IllegalArgumentException("invalid ordinal: " + ordinal);
        }

        return schema.at(ordinal).getDataType();
    }

    private void throwIfUnsafeAccess(
        int ordinal, Class<? extends DataType> expDataType, String accessType)
    {

        DataType actualDataType = dataType(ordinal);
        if (!expDataType.isAssignableFrom(actualDataType.getClass())) {
            String msg = String.format(
                "Trying to access a `%s` value from vector of type `%s`",
                accessType,
                actualDataType);
            throw new UnsupportedOperationException(msg);
        }
    }

    private static int getOrdinal(String columnName)
    {
        return colNameToOrdinal.get(columnName);
    }
}
