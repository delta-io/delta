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

import org.apache.commons.cli.Options;

import io.delta.kernel.*;
import io.delta.kernel.utils.CloseableIterable;
import static io.delta.kernel.examples.utils.Utils.parseArgs;

/**
 * Example program to create a Delta table (no data is written) using the Kernel APIs.
 * <p>
 * Creates two tables with the following schema and partition columns in the input given directory
 * location.
 * <pre>
 *     Table 1: un-partitioned table
 *     CREATE TABLE example (id INT, name STRING, address STRING, salary DECIMAL(1, 3))
 *
 *     Table 2: partitioned table
 *     CREATE TABLE example_partitioned (id INT, name STRING, salary DECIMAL(1, 3), city STRING))
 *     PARTITIONED BY (city)
 * </pre>
 * <p>
 * <p>
 * It prints the table locations at the end of the successful execution.
 */
public class CreateTable extends BaseTableWriter {
    public static void main(String[] args)
            throws Exception {
        Options options = new Options()
                .addOption("l", "location", true, "Locations where the sample tables are created");

        new CreateTable().runExamples(parseArgs(options, args).getOptionValue("location"));
    }

    public void runExamples(String location) {
        createUnpartitionedTable(location + "/example");
        createPartitionedTable(location + "/example_partitioned");
    }

    public void createUnpartitionedTable(String tablePath) {
        // Create a `Table` object with the given destination table path
        Table table = Table.forPath(engine, tablePath);

        // Create a transaction builder to build the transaction
        TransactionBuilder txnBuilder =
                table.createTransactionBuilder(
                        engine,
                        "Examples", /* engineInfo */
                        Operation.CREATE_TABLE);

        // Set the schema of the new table on the transaction builder
        txnBuilder = txnBuilder.withSchema(engine, exampleTableSchema);

        // Build the transaction
        Transaction txn = txnBuilder.build(engine);

        // Commit the transaction.
        // As we are just creating the table and not adding any data, the `dataActions` is empty.
        TransactionCommitResult commitResult =
                txn.commit(
                        engine,
                        CloseableIterable.emptyIterable() /* dataActions */);

        // Check the transaction commit result
        verifyCommitSuccess(tablePath, commitResult);
    }

    public void createPartitionedTable(String tablePath) {
        // Create a `Table` object with the given destination table path
        Table table = Table.forPath(engine, tablePath);

        // Create a transaction builder to build the transaction
        TransactionBuilder txnBuilder =
                table.createTransactionBuilder(
                        engine,
                        "Examples", /* engineInfo */
                        Operation.CREATE_TABLE);

        txnBuilder = txnBuilder
                // Set the schema of the new table
                .withSchema(engine, examplePartitionedTableSchema)
                // set the partition columns of the new table
                .withPartitionColumns(engine, examplePartitionColumns);

        // Build the transaction
        Transaction txn = txnBuilder.build(engine);

        // Commit the transaction.
        // As we are just creating the table and not adding any data, the `dataActions` is empty.
        TransactionCommitResult commitResult =
                txn.commit(
                        engine,
                        CloseableIterable.emptyIterable() /* dataActions */);

        // Check the transaction commit result
        verifyCommitSuccess(tablePath, commitResult);
    }
}
