/*
 * Copyright (2024) The Delta Lake Project Authors.
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
import java.util.*;

import org.apache.commons.cli.Options;

import io.delta.kernel.*;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.utils.*;
import static io.delta.kernel.examples.utils.Utils.parseArgs;

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

/**
 * Example program that demonstrates how to:
 *
 * <ul>
 *     <li>
 *         create a partiitoned and unpartitioned table and insert data into it
 *         (Basically the CREATE TABLE AS <query> command).
 *     </li>
 *     <li>
 *          Insert into an existing table
 *      </li>
 *      <li>
 *          Idempotent data write to a table.
 *      </li>
 * </ul>
 */
public class CreateTableAndInsertData extends BaseTableWriter {
    public static void main(String[] args) throws IOException {
        Options options = new Options()
                .addOption("l", "location", true, "Locations where the sample tables are created");

        new CreateTableAndInsertData().runExamples(
                parseArgs(options, args).getOptionValue("location"));

    }

    public void runExamples(String location) throws IOException {
        String unpartitionedTblPath = location + "/example";
        String partitionTblPath = location + "/example_partitioned";

        // CTAS example for unpartitioned tables
        createTableWithSampleData(unpartitionedTblPath);

        // CTAS example for partitioned tables
        createPartitionedTableWithSampleData(partitionTblPath);

        // Insert into an existing table.
        insertDataIntoUnpartitionedTable(unpartitionedTblPath);

        // Example of idempotent inserts
        idempotentInserts(unpartitionedTblPath);

        // Example of checkpointg
        insertWithOptionalCheckpoint(unpartitionedTblPath);
    }

    public TransactionCommitResult createTableWithSampleData(String tablePath) throws IOException {
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

        // Get the transaction state
        Row txnState = txn.getTransactionState(engine);

        // Generate the sample data for the table that confirms to the table schema
        FilteredColumnarBatch batch1 = generateUnpartitionedDataBatch(5 /* offset */);
        FilteredColumnarBatch batch2 = generateUnpartitionedDataBatch(10 /* offset */);
        FilteredColumnarBatch batch3 = generateUnpartitionedDataBatch(25 /* offset */);
        CloseableIterator<FilteredColumnarBatch> data =
                toCloseableIterator(Arrays.asList(batch1, batch2, batch3).iterator());

        // First transform the logical data to physical data that needs to be written to the Parquet
        // files
        CloseableIterator<FilteredColumnarBatch> physicalData =
                Transaction.transformLogicalData(
                        engine,
                        txnState,
                        data,
                        // partition values - as this table is unpartitioned, it should be empty
                        Collections.emptyMap());

        // Get the write context
        DataWriteContext writeContext = Transaction.getWriteContext(
                engine,
                txnState,
                // partition values - as this table is unpartitioned, it should be empty
                Collections.emptyMap());


        // Now write the physical data to Parquet files
        CloseableIterator<DataFileStatus> dataFiles = engine.getParquetHandler()
                .writeParquetFiles(
                        writeContext.getTargetDirectory(),
                        physicalData,
                        writeContext.getStatisticsColumns());


        // Now convert the data file status to data actions that needs to be written to the Delta
        // table log
        CloseableIterator<Row> dataActions =
                Transaction.generateAppendActions(engine, txnState, dataFiles, writeContext);


        // Create a iterable out of the data actions. If the contents are too big to fit in memory,
        // the connector may choose to write the data actions to a temporary file and return an
        // iterator that reads from the file.
        CloseableIterable<Row> dataActionsIterable =
                CloseableIterable.inMemoryIterable(dataActions);

        // Commit the transaction.
        TransactionCommitResult commitResult = txn.commit(engine, dataActionsIterable);

        // Check the transaction commit result
        verifyCommitSuccess(tablePath, commitResult);

        return commitResult;
    }

    public TransactionCommitResult createPartitionedTableWithSampleData(String tablePath) throws IOException {
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

        // Get the transaction state
        Row txnState = txn.getTransactionState(engine);

        List<Row> dataActions = new ArrayList<>();

        // Generate the sample data for three partitions. Process each partition separately.
        // This is just an example. In a real-world scenario, the data may come from different
        // partitions. Connectors already have the capability to partition by partition values
        // before writing to the table

        // In the test data `city` is a partition column
        for (String city : Arrays.asList("San Francisco", "Campbell", "San Jose")) {
            FilteredColumnarBatch batch1 = generatedPartitionedDataBatch(
                    5 /* offset */, city /* partition value */);
            FilteredColumnarBatch batch2 = generatedPartitionedDataBatch(
                    5 /* offset */, city /* partition value */);
            FilteredColumnarBatch batch3 = generatedPartitionedDataBatch(
                    10 /* offset */, city /* partition value */);

            CloseableIterator<FilteredColumnarBatch> data =
                    toCloseableIterator(Arrays.asList(batch1, batch2, batch3).iterator());

            // Create partition value map
            Map<String, Literal> partitionValues =
                    Collections.singletonMap(
                            "city", // partition column name
                            // partition value. Depending upon the parition column type, the
                            // partition value should be created. In this example, the partition
                            // column is of type StringType, so we are creating a string literal.
                            Literal.ofString(city, "UTF8_BINARY"));


            // First transform the logical data to physical data that needs to be written
            // to the Parquet
            // files
            CloseableIterator<FilteredColumnarBatch> physicalData =
                    Transaction.transformLogicalData(engine, txnState, data, partitionValues);

            // Get the write context
            DataWriteContext writeContext =
                    Transaction.getWriteContext(engine, txnState, partitionValues);


            // Now write the physical data to Parquet files
            CloseableIterator<DataFileStatus> dataFiles = engine.getParquetHandler()
                    .writeParquetFiles(
                            writeContext.getTargetDirectory(),
                            physicalData,
                            writeContext.getStatisticsColumns());


            // Now convert the data file status to data actions that needs to be written to the Delta
            // table log
            CloseableIterator<Row> partitionDataActions = Transaction.generateAppendActions(
                    engine,
                    txnState,
                    dataFiles,
                    writeContext);

            // Now add all the partition data actions to the main data actions list. In a
            // distributed query engine, the partition data is written to files at tasks on executor
            // nodes. The data actions are collected at the driver node and then written to the
            // Delta table log using the `Transaction.commit`
            while (partitionDataActions.hasNext()) {
                dataActions.add(partitionDataActions.next());
            }
        }


        // Create a iterable out of the data actions. If the contents are too big to fit in memory,
        // the connector may choose to write the data actions to a temporary file and return an
        // iterator that reads from the file.
        CloseableIterable<Row> dataActionsIterable = CloseableIterable.inMemoryIterable(
                toCloseableIterator(dataActions.iterator()));

        // Commit the transaction.
        TransactionCommitResult commitResult = txn.commit(engine, dataActionsIterable);

        // Check the transaction commit result
        verifyCommitSuccess(tablePath, commitResult);

        return commitResult;
    }


    public TransactionCommitResult insertDataIntoUnpartitionedTable(String tablePath) throws IOException {
        // Create a `Table` object with the given destination table path
        Table table = Table.forPath(engine, tablePath);

        // Create a transaction builder to build the transaction
        TransactionBuilder txnBuilder =
                table.createTransactionBuilder(
                        engine,
                        "Examples", /* engineInfo */
                        Operation.CREATE_TABLE);

        // Build the transaction - no need to provide the schema as the table already exists.
        Transaction txn = txnBuilder.build(engine);

        // Get the transaction state
        Row txnState = txn.getTransactionState(engine);

        // Generate the sample data for the table that confirms to the table schema
        FilteredColumnarBatch batch1 = generateUnpartitionedDataBatch(5 /* offset */);
        FilteredColumnarBatch batch2 = generateUnpartitionedDataBatch(10 /* offset */);
        FilteredColumnarBatch batch3 = generateUnpartitionedDataBatch(25 /* offset */);
        CloseableIterator<FilteredColumnarBatch> data =
                toCloseableIterator(Arrays.asList(batch1, batch2, batch3).iterator());

        // First transform the logical data to physical data that needs to be written to the Parquet
        // files
        CloseableIterator<FilteredColumnarBatch> physicalData =
                Transaction.transformLogicalData(
                        engine,
                        txnState,
                        data,
                        // partition values - as this table is unpartitioned, it should be empty
                        Collections.emptyMap());

        // Get the write context
        DataWriteContext writeContext = Transaction.getWriteContext(
                engine,
                txnState,
                // partition values - as this table is unpartitioned, it should be empty
                Collections.emptyMap());


        // Now write the physical data to Parquet files
        CloseableIterator<DataFileStatus> dataFiles = engine.getParquetHandler()
                .writeParquetFiles(
                        writeContext.getTargetDirectory(),
                        physicalData,
                        writeContext.getStatisticsColumns());


        // Now convert the data file status to data actions that needs to be written to the Delta
        // table log
        CloseableIterator<Row> dataActions =
                Transaction.generateAppendActions(engine, txnState, dataFiles, writeContext);


        // Create a iterable out of the data actions. If the contents are too big to fit in memory,
        // the connector may choose to write the data actions to a temporary file and return an
        // iterator that reads from the file.
        CloseableIterable<Row> dataActionsIterable =
                CloseableIterable.inMemoryIterable(dataActions);

        // Commit the transaction.
        TransactionCommitResult commitResult = txn.commit(engine, dataActionsIterable);

        // Check the transaction commit result
        verifyCommitSuccess(tablePath, commitResult);

        return commitResult;
    }

    public TransactionCommitResult idempotentInserts(String tablePath) throws IOException {
        // Create a `Table` object with the given destination table path
        Table table = Table.forPath(engine, tablePath);

        // Create a transaction builder to build the transaction
        TransactionBuilder txnBuilder =
                table.createTransactionBuilder(
                        engine,
                        "Examples", /* engineInfo */
                        Operation.CREATE_TABLE);

        // Set the transaction identifiers for idempotent writes
        // Delta/Kernel makes sure that there exists only one transaction in the Delta log
        // with the given application id and txn version
        txnBuilder = txnBuilder.withTransactionId(
                engine,
                "my app id", /* application id */
                100 /* txn version */);

        // Build the transaction - no need to provide the schema as the table already exists.
        Transaction txn = txnBuilder.build(engine);

        // Get the transaction state
        Row txnState = txn.getTransactionState(engine);

        // Generate the sample data for the table that confirms to the table schema
        FilteredColumnarBatch batch1 = generateUnpartitionedDataBatch(5 /* offset */);
        FilteredColumnarBatch batch2 = generateUnpartitionedDataBatch(10 /* offset */);
        FilteredColumnarBatch batch3 = generateUnpartitionedDataBatch(25 /* offset */);
        CloseableIterator<FilteredColumnarBatch> data =
                toCloseableIterator(Arrays.asList(batch1, batch2, batch3).iterator());

        // First transform the logical data to physical data that needs to be written to the Parquet
        // files
        CloseableIterator<FilteredColumnarBatch> physicalData =
                Transaction.transformLogicalData(
                        engine,
                        txnState,
                        data,
                        // partition values - as this table is unpartitioned, it should be empty
                        Collections.emptyMap());

        // Get the write context
        DataWriteContext writeContext = Transaction.getWriteContext(
                engine,
                txnState,
                // partition values - as this table is unpartitioned, it should be empty
                Collections.emptyMap());


        // Now write the physical data to Parquet files
        CloseableIterator<DataFileStatus> dataFiles = engine.getParquetHandler()
                .writeParquetFiles(
                        writeContext.getTargetDirectory(),
                        physicalData,
                        writeContext.getStatisticsColumns());


        // Now convert the data file status to data actions that needs to be written to the Delta
        // table log
        CloseableIterator<Row> dataActions =
                Transaction.generateAppendActions(engine, txnState, dataFiles, writeContext);


        // Create a iterable out of the data actions. If the contents are too big to fit in memory,
        // the connector may choose to write the data actions to a temporary file and return an
        // iterator that reads from the file.
        CloseableIterable<Row> dataActionsIterable =
                CloseableIterable.inMemoryIterable(dataActions);

        // Commit the transaction.
        TransactionCommitResult commitResult = txn.commit(engine, dataActionsIterable);

        // Check the transaction commit result
        verifyCommitSuccess(tablePath, commitResult);
        return commitResult;
    }

    public void insertWithOptionalCheckpoint(String tablePath) throws IOException {
        boolean didCheckpoint = false;
        // insert data multiple times to trigger a checkpoint. By default checkpoint is needed
        // for every 10 versions.
        for (int i = 0; i < 12; i++) {
            TransactionCommitResult commitResult = insertDataIntoUnpartitionedTable(tablePath);
            if (commitResult.isReadyForCheckpoint()) {
                // Checkpoint the table
                Table.forPath(engine, tablePath).checkpoint(engine, commitResult.getVersion());
                didCheckpoint = true;
            }
        }

        if (!didCheckpoint) {
            throw new RuntimeException("Table should have checkpointed by now");
        }
    }
}
