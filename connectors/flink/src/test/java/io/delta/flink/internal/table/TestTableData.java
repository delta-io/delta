package io.delta.flink.internal.table;

import java.util.stream.IntStream;

import io.delta.flink.sink.internal.SchemaConverter;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;

import io.delta.standalone.types.StructField;

/**
 * Dictionary class for Table tests. Contains information about column names and types used for
 * Table tests.
 */
public final class TestTableData {

    public static final String[] COLUMN_NAMES = new String[] {"col1", "col2", "col3"};

    /**
     * Flink data types for {@link TestTableData#COLUMN_NAMES} columns.
     */
    public static final DataType[] COLUMN_TYPES = new DataType[] {
        new AtomicDataType(new BooleanType()),
        new AtomicDataType(new IntType()),
        new AtomicDataType(new VarCharType())
    };

    /**
     * Delta Table scheme created based on {@link TestTableData#COLUMN_NAMES} and {@link
     * TestTableData#COLUMN_TYPES}
     */
    public static final StructField[] DELTA_FIELDS = IntStream.range(0, COLUMN_NAMES.length)
        .mapToObj(value -> new StructField(COLUMN_NAMES[value],
            SchemaConverter.toDeltaDataType(COLUMN_TYPES[value].getLogicalType())))
        .toArray(StructField[]::new);

}
