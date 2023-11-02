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
package io.delta.kernel.examples;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;

import io.delta.kernel.TableNotFoundException;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;

import io.delta.kernel.defaults.client.DefaultTableClient;

/**
 * Base class for reading Delta Lake tables using the Delta Kernel APIs.
 */
public abstract class BaseTableReader {
    public static final int DEFAULT_LIMIT = 20;

    protected final String tablePath;
    protected final TableClient tableClient;

    public BaseTableReader(String tablePath) {
        this.tablePath = requireNonNull(tablePath);
        this.tableClient = DefaultTableClient.create(new Configuration());
    }

    /**
     * Show the given {@code limit} rows containing the given columns with the predicate from the
     * table.
     *
     * @param limit        Max number of rows to show.
     * @param columnsOpt   If null, show all columns in the table.
     * @param predicateOpt Optional predicate
     * @return Number of rows returned by the query.
     * @throws TableNotFoundException
     * @throws IOException
     */
    public abstract int show(
        int limit,
        Optional<List<String>> columnsOpt,
        Optional<Predicate> predicateOpt) throws TableNotFoundException, IOException;

    /**
     * Utility method to return a pruned schema that contains the given {@code columns} from
     * {@code baseSchema}
     */
    protected static StructType pruneSchema(StructType baseSchema, Optional<List<String>> columns) {
        if (!columns.isPresent()) {
            return baseSchema;
        }
        List<StructField> selectedFields = columns.get().stream().map(column -> {
            if (baseSchema.indexOf(column) == -1) {
                throw new IllegalArgumentException(
                    format("Column %s is not found in table", column));
            }
            return baseSchema.get(column);
        }).collect(Collectors.toList());

        return new StructType(selectedFields);
    }

    protected static int printData(FilteredColumnarBatch data, int maxRowsToPrint) {
        int printedRowCount = 0;
        try (CloseableIterator<Row> rows = data.getRows()) {
            while (rows.hasNext()) {
                printRow(rows.next());
                printedRowCount++;
                if (printedRowCount == maxRowsToPrint) {
                    break;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return printedRowCount;
    }

    protected static void printSchema(StructType schema) {
        System.out.printf(formatter(schema.length()), schema.fieldNames().toArray(new String[0]));
    }

    protected static void printRow(Row row){
        int numCols = row.getSchema().length();
        Object[] rowValues = IntStream.range(0, numCols)
            .mapToObj(colOrdinal -> getValue(row, colOrdinal))
            .toArray();

        // TODO: Need to handle the Row, Map, Array, Timestamp, Date types specially to
        // print them in the format they need. Copy this code from Spark CLI.

        System.out.printf(formatter(numCols), rowValues);
    }

    /**
     * Minimum command line options for any implementation of this reader.
     */
    protected static Options baseOptions() {
        return new Options()
            .addRequiredOption("t", "table", true, "Fully qualified table path")
            .addOption("c", "columns", true,
                "Comma separated list of columns to read from the table. " +
                    "Ex. --columns=id,name,address")
            .addOption(
                Option.builder()
                    .option("l")
                    .longOpt("limit")
                    .hasArg(true)
                    .desc("Maximum number of rows to read from the table (default 20).")
                    .type(Number.class)
                    .build()
            );
    }

    /**
     * Helper method to parse the command line arguments.
     */
    protected static CommandLine parseArgs(Options options, String[] args) {
        CommandLineParser cliParser = new DefaultParser();

        try {
            return cliParser.parse(options, args);
        } catch (ParseException parseException) {
            new HelpFormatter().printHelp(
                "java " + SingleThreadedTableReader.class.getCanonicalName(),
                options,
                true
            );
        }
        System.exit(-1);
        return null;
    }

    protected static Optional<List<String>> parseColumnList(CommandLine cli, String optionName) {
        return Optional.ofNullable(cli.getOptionValue(optionName))
            .map(colString -> Arrays.asList(colString.split(",[ ]*")));
    }

    protected static int parseInt(CommandLine cli, String optionName, int defaultValue)
        throws ParseException {
        return Optional.ofNullable(cli.getParsedOptionValue(optionName))
            .map(Number.class::cast)
            .map(Number::intValue)
            .orElse(defaultValue);
    }

    private static String formatter(int length) {
        return IntStream.range(0, length)
            .mapToObj(i -> "%20s")
            .collect(Collectors.joining("|")) + "\n";
    }

    private static String getValue(Row row, int columnOrdinal) {
        DataType dataType = row.getSchema().at(columnOrdinal).getDataType();
        if (row.isNullAt(columnOrdinal)) {
            return null;
        } else if (dataType instanceof BooleanType) {
            return Boolean.toString(row.getBoolean(columnOrdinal));
        } else if (dataType instanceof ByteType) {
            return Byte.toString(row.getByte(columnOrdinal));
        } else if (dataType instanceof ShortType) {
            return Short.toString(row.getShort(columnOrdinal));
        } else if (dataType instanceof IntegerType) {
            return Integer.toString(row.getInt(columnOrdinal));
        } else if (dataType instanceof DateType) {
            // DateType data is stored internally as the number of days since 1970-01-01
            int daysSinceEpochUTC = row.getInt(columnOrdinal);
            return LocalDate.ofEpochDay(daysSinceEpochUTC).toString();
        } else if (dataType instanceof LongType) {
            return Long.toString(row.getLong(columnOrdinal));
        } else if (dataType instanceof TimestampType) {
            // TimestampType data is stored internally as the number of microseconds since epoch
            long microSecsSinceEpochUTC = row.getLong(columnOrdinal);
            LocalDateTime dateTime = LocalDateTime.ofEpochSecond(
                microSecsSinceEpochUTC / 1_000_000 /* epochSecond */,
                (int) (1000 * microSecsSinceEpochUTC % 1_000_000) /* nanoOfSecond */,
                ZoneOffset.UTC);
            return dateTime.toString();
        } else if (dataType instanceof FloatType) {
            return Float.toString(row.getFloat(columnOrdinal));
        } else if (dataType instanceof DoubleType) {
            return Double.toString(row.getDouble(columnOrdinal));
        } else if (dataType instanceof StringType) {
            return row.getString(columnOrdinal);
        } else if (dataType instanceof BinaryType) {
            return new String(row.getBinary(columnOrdinal));
        } else if (dataType instanceof DecimalType) {
            return row.getDecimal(columnOrdinal).toString();
        } else if (dataType instanceof StructType) {
            return "TODO: struct value";
        } else if (dataType instanceof ArrayType) {
            return "TODO: list value";
        } else if (dataType instanceof MapType) {
            return "TODO: map value";
        } else {
            throw new UnsupportedOperationException("unsupported data type: " + dataType);
        }
    }
}

