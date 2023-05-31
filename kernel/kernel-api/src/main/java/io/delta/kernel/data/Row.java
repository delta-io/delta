package io.delta.kernel.data;

import io.delta.kernel.types.StructType;

import java.util.List;
import java.util.Map;

/**
 * Represent a single record
 */
public interface Row {

    /**
     * @return Schema of the record.
     */
    StructType getSchema();

    /**
     * Is the column at given ordinal has null value?
     * @param ordinal
     * @return true if the column at the given ordinal is null.
     */
    boolean isNullAt(int ordinal);

    /**
     * Return boolean value of the column located at the given ordinal.
     * Throws error if the column at given ordinal is not of boolean type,
     * @param ordinal
     * @return
     */
    boolean getBoolean(int ordinal);

    /**
     * Return integer value of the column located at the given ordinal.
     * Throws error if the column at given ordinal is not of integer type,
     * @param ordinal
     * @return
     */
    int getInt(int ordinal);

    /**
     * Return long value of the column located at the given ordinal.
     * Throws error if the column at given ordinal is not of long type,
     * @param ordinal
     * @return
     */
    long getLong(int ordinal);

    /**
     * Return string value of the column located at the given ordinal.
     * Throws error if the column at given ordinal is not of varchar type,
     * @param ordinal
     * @return
     */
    String getString(int ordinal);

    /**
     * Return struct value of the column located at the given ordinal.
     * Throws error if the column at given ordinal is not of struct type,
     * @param ordinal
     * @return
     */
    Row getRecord(int ordinal);

    /**
     * Return array value of the column located at the given ordinal.
     * Throws error if the column at given ordinal is not of array type,
     * @param ordinal
     * @return
     */
    <T> List<T> getList(int ordinal);

    /**
     * Return map value of the column located at the given ordinal.
     * Throws error if the column at given ordinal is not of map type,
     * @param ordinal
     * @return
     */
    <K, V> Map<K, V> getMap(int ordinal);
}
