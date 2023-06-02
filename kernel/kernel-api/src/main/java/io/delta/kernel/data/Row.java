package io.delta.kernel.data;

import java.util.List;
import java.util.Map;

import io.delta.kernel.types.StructType;

/**
 * Represent a single record
 */
public interface Row {

    /**
     * @return Schema of the record.
     */
    StructType getSchema();

    /**
     * @param ordinal the ordinal of the column to check
     * @return whether the column at {@code ordinal} is null
     */
    boolean isNullAt(int ordinal);

    /**
     * Return boolean value of the column located at the given ordinal.
     * Throws error if the column at given ordinal is not of boolean type,
     */
    boolean getBoolean(int ordinal);

    /**
     * Return integer value of the column located at the given ordinal.
     * Throws error if the column at given ordinal is not of integer type,
     */
    int getInt(int ordinal);

    /**
     * Return long value of the column located at the given ordinal.
     * Throws error if the column at given ordinal is not of long type,
     */
    long getLong(int ordinal);

    /**
     * Return string value of the column located at the given ordinal.
     * Throws error if the column at given ordinal is not of varchar type,
     */
    String getString(int ordinal);

    /**
     * Return struct value of the column located at the given ordinal.
     * Throws error if the column at given ordinal is not of struct type,
     */
    Row getRecord(int ordinal);

    /**
     * Return array value of the column located at the given ordinal.
     * Throws error if the column at given ordinal is not of array type,
     */
    <T> List<T> getList(int ordinal);

    /**
     * Return map value of the column located at the given ordinal.
     * Throws error if the column at given ordinal is not of map type,
     */
    <K, V> Map<K, V> getMap(int ordinal);
}
