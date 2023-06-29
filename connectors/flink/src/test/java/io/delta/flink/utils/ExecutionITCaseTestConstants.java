package io.delta.flink.utils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

public final class ExecutionITCaseTestConstants {

    private ExecutionITCaseTestConstants() {

    }

    public static final LogicalType[] DATA_COLUMN_TYPES =
        {new CharType(), new CharType(), new IntType()};

    public static final List<String> NAME_COLUMN_VALUES =
        Stream.of("Jan", "Jan").collect(Collectors.toList());

    public static final Set<String> SURNAME_COLUMN_VALUES =
        Stream.of("Kowalski", "Duda").collect(Collectors.toSet());

    public static final Set<Integer> AGE_COLUMN_VALUES =
        Stream.of(1, 2).collect(Collectors.toSet());

    public static final String[] ALL_DATA_TABLE_COLUMN_NAMES = {
        "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10"
    };

    public static final LogicalType[] ALL_DATA_TABLE_COLUMN_TYPES = {
        new TinyIntType(), new SmallIntType(), new IntType(), new DoubleType(), new FloatType(),
        new DecimalType(), new DecimalType(), new TimestampType(), new VarCharType(),
        new BooleanType()
    };

    public static final int ALL_DATA_TABLE_RECORD_COUNT = 5;

    /**
     * Columns that are not used as a partition columns.
     */
    public static final String[] DATA_COLUMN_NAMES = {"name", "surname", "age"};

    // Large table has no partitions.
    public static final String[] LARGE_TABLE_ALL_COLUMN_NAMES = {"col1", "col2", "col3"};

    public static final LogicalType[] LARGE_TABLE_ALL_COLUMN_TYPES =
        {new BigIntType(), new BigIntType(), new VarCharType()};

    public static final int SMALL_TABLE_COUNT = 2;

    public static final int LARGE_TABLE_RECORD_COUNT = 1100;

}
