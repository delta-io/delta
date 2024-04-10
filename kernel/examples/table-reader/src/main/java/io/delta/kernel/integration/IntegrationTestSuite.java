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
package io.delta.kernel.integration;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static java.util.Arrays.asList;

import io.delta.kernel.examples.SingleThreadedTableReader;
import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;

/**
 * Test suite that runs various integration tests for sanity testing the staged/released artifacts.
 * It only verifies the number of rows in the results and not the specific values of rows.
 * For full scale results verification we rely on unit tests which are run as part of the CI jobs.
 */
public class IntegrationTestSuite {
    private final String goldenTableDir;

    public static void main(String[] args) throws Exception {
        new IntegrationTestSuite(args[0])
            .runTests();
    }

    public IntegrationTestSuite(String goldenTableDir) {
        this.goldenTableDir = goldenTableDir;
    }

    public void runTests() throws Exception {
        // Definitions of golden tables is present in
        // <root>/connectors/golden-tables/src/test/scala/io/delta/golden/GoldenTables.scala

        // Basic reads: Simple table
        runAndVerifyRowCount(
            "basic_read_simple_table",
            "data-reader-primitives",
            Optional.empty(), /* read schema - read all columns */
            Optional.empty(), /* predicate */
            11 /* expected row count */);

        // Basic reads: Partitioned table
        runAndVerifyRowCount(
            "basic_read_partitioned_table",
            "data-reader-array-primitives",
            Optional.empty(), /* read schema - read all columns */
            Optional.empty(), /* predicate */
            10 /* expected row count */);

        // Basic reads: Table with DVs
        runAndVerifyRowCount(
            "basic_read_table_with_deletionvectors",
            "dv-partitioned-with-checkpoint",
            Optional.empty(), /* read schema - read all columns */
            Optional.empty(), /* predicate */
            35 /* expected row count */);

        // Basic reads: select subset of columns
        runAndVerifyRowCount(
            "basic_read_subset_of_columns",
            "dv-partitioned-with-checkpoint",
            Optional.of(asList("part", "col2")), /* read schema */
            Optional.empty(), /* predicate */
            35 /* expected row count */);

        // Basic reads: Table with DVs and column mapping name
        runAndVerifyRowCount(
            "basic_read_table_with_columnmapping_deletionvectors",
            "dv-with-columnmapping",
            Optional.of(asList("col1", "col2")), /* read schema */
            Optional.empty(), /* predicate */
            35 /* expected row count */);

        // Basic read: table with column mapping mode id
        runAndVerifyRowCount(
            "basic_read_table_columnmapping_id",
            "table-with-columnmapping-mode-id",
            Optional.of(
                asList("ByteType", "decimal", "nested_struct", "array_of_prims", "map_of_prims")),
            Optional.empty(), /* predicate */
            6 /* expected row count */);

        // Partition pruning: simple expression
        runAndVerifyRowCount(
            "partition_pruning_simple_filter",
            "basic-decimal-table",
            Optional.empty(), /* read schema - read all columns */
            Optional.of(new Predicate(
                "=",
                asList(
                    new Column("part"),
                    Literal.ofDecimal(new BigDecimal("2342222.23454"), 12, 5)))),
            1 /* expected row count */);

        // Partition pruning: simple expression where nothing is pruned
        runAndVerifyRowCount(
            "partition_pruning_simple_filter_no_pruning",
            "basic-decimal-table",
            Optional.empty(), /* read schema - read all columns */
            Optional.of(
                new Predicate(
                    "NOT",
                    asList(
                        new Predicate(
                            "=",
                            asList(new Column("part"), Literal.ofDecimal(new BigDecimal(0), 12, 5)))
                    ))),
            4 /* expected row count */);

        // Partition pruning + data skipping: filter on data and metadata columns where
        // data filter doesn't prune anything
        runAndVerifyRowCount(
            "partition_pruning_filter_on_data_and_metadata_columns_1",
            "dv-partitioned-with-checkpoint",
            Optional.of(asList("part", "col2")), /* read schema */
            Optional.of(
                new And(
                    new Predicate(">=", asList(new Column("part"), Literal.ofInt(7))),
                    new Predicate(">=", asList(new Column("col1"), Literal.ofInt(0))))),
            12 /* expected row count */);

        // Partition pruning + data skipping: filter on data and metadata columns where
        // data filter also prunes few files based on the stats based skipping
        runAndVerifyRowCount(
            "partition_pruning_filter_on_data_and_metadata_columns_2",
            "dv-partitioned-with-checkpoint",
            Optional.of(asList("part", "col2")), /* read schema */
            Optional.of(
                new And(
                    new Predicate(">=", asList(new Column("part"), Literal.ofInt(7))),
                    new Predicate("=", asList(new Column("col1"), Literal.ofInt(28))))),
            5 /* expected row count */);

        // Data skipping: filter on a table with checkpoint
        runAndVerifyRowCount(
            "data_skipping_table_with_checkpoint",
            "data-skipping-basic-stats-all-types-checkpoint",
            Optional.empty(), /* read schema - read all columns */
            Optional.of(
                new Predicate(
                    ">",
                    asList(new Column("as_int"), Literal.ofInt(0))
                )),
            0 /* expected row count */);

        // Partition pruning: table with column mapping mode name
        runAndVerifyRowCount(
            "partition_pruning_columnmapping_name",
            "dv-with-columnmapping",
            Optional.empty(),
            Optional.of(
                new Predicate(
                    "=",
                    asList(new Column("part"), Literal.ofInt(0))
                )),
            2 /* expected row count */);

        // Data skipping: table with column mapping mode id
        runAndVerifyRowCount(
            "data_skipping_columnmapping_id",
            "data-skipping-basic-stats-all-types-columnmapping-id",
            Optional.empty(),
            Optional.of(
                new Predicate(
                    "=",
                    asList(new Column("as_int"), Literal.ofInt(1))
                )),
            0 /* expected row count */);

        // Type widening: table with multiple type changes.
        runAndVerifyRowCount(
            "type_widening",
            "type-widening",
            Optional.empty(), /* read schema - read all columns */
            Optional.empty(), /* predicate */
            3 /* expected row count */);
    }

    private void runAndVerifyRowCount(
        String testName,
        String goldenTable,
        Optional<List<String>> readColumns,
        Optional<Predicate> predicate,
        int expectedRowCount) throws Exception {
        System.out.println("\n========== TEST START: " + testName + " ==============");
        try {
            String path = goldenTableDir + "/" + goldenTable;
            SingleThreadedTableReader reader = new SingleThreadedTableReader(path);
            // Select a large number of rows (1M), so that everything in the table is read.
            int actRowCount = reader.show(1_000_000, readColumns, predicate);
            if (actRowCount != expectedRowCount) {
                throw new RuntimeException(String.format(
                    "Test (%s) failed: expected row count = %s, actual row count = %s",
                    testName, expectedRowCount, actRowCount));
            }
        } finally {
            System.out.println("========== TEST END: " + testName + " ==============\n");
        }
    }
}
