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
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.DataReadResult;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import io.delta.kernel.defaults.client.DefaultTableClient;
import io.delta.kernel.defaults.internal.data.vector.VectorUtils;

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
     * Show the given {@code limit} rows containing the given columns from the table.
     *
     * @param limit      Max number of rows to show.
     * @param columnsOpt If null, show all columns in the table.
     * @throws TableNotFoundException
     * @throws IOException
     */
    public abstract void show(int limit, Optional<List<String>> columnsOpt)
        throws TableNotFoundException, IOException;

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

    protected static int printData(DataReadResult dataReadResult, int maxRowsToPrint) {
        int printedRowCount = 0;
        ColumnarBatch data = dataReadResult.getData();
        Optional<ColumnVector> selectionVector = dataReadResult.getSelectionVector();
        for (int rowId = 0; rowId < data.getSize(); rowId++) {
            if (!selectionVector.isPresent() || selectionVector.get().getBoolean(rowId)) {
                printRow(data, rowId);
                printedRowCount++;
                if (printedRowCount == maxRowsToPrint) {
                    break;
                }
            }
        }
        return printedRowCount;
    }

    protected static void printSchema(StructType schema) {
        System.out.printf(formatter(schema.length()), schema.fieldNames().toArray(new String[0]));
    }

    protected static void printRow(ColumnarBatch batch, int rowId) {
        int numCols = batch.getSchema().length();
        Object[] rowValues = IntStream.range(0, numCols).mapToObj(colOrdinal -> {
            ColumnVector columnVector = batch.getColumnVector(colOrdinal);
            return VectorUtils.getValueAsObject(columnVector, rowId);
        }).toArray();

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
}

