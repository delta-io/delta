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

import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import java.util.UUID;

import io.delta.kernel.examples.*;

/**
 * Test suite that runs various integration tests for sanity testing the staged/released artifacts.
 * It only verifies the number of rows in the results and not the specific values of rows. For full
 * scale results verification we rely on unit tests which are run as part of the CI jobs.
 */
public class WriteIntegrationTestSuite {

    public static void main(String[] args) throws Exception {
        new WriteIntegrationTestSuite().runTests();
    }


    public void runTests() throws Exception {
        verifyRowCount(
                "Create un-partitioned table",
                0 /* expected row count */,
                tblLocation -> new CreateTable().createUnpartitionedTable(tblLocation)
        );

        verifyRowCount(
                "Create partitioned table",
                0 /* expected row count */,
                tblLocation -> new CreateTable().createPartitionedTable(tblLocation)
        );

        verifyRowCount(
                "Create un-partitioned table and insert data",
                15 /* expected row count */,
                tblLocation -> new CreateTableAndInsertData().createTableWithSampleData(tblLocation)
        );

        verifyRowCount(
                "Create partitioned table and insert data",
                45 /* expected row count */,
                tblLocation ->
                        new CreateTableAndInsertData()
                                .createPartitionedTableWithSampleData(tblLocation)
        );

        verifyRowCount(
                "insert data into an existing table",
                30 /* expected row count */,
                tblLocation -> {
                    CreateTableAndInsertData createTableAndInsertData =
                            new CreateTableAndInsertData();
                    createTableAndInsertData.createTableWithSampleData(tblLocation);
                    createTableAndInsertData.insertDataIntoUnpartitionedTable(tblLocation);
                });

        verifyRowCount(
                "idempotent inserts into a table",
                30 /* expected row count */,
                tblLocation -> {
                    CreateTableAndInsertData createTableAndInsertData =
                            new CreateTableAndInsertData();
                    createTableAndInsertData.createTableWithSampleData(tblLocation);
                    createTableAndInsertData.idempotentInserts(tblLocation);
                });


        verifyRowCount(
                "inserts with an optional checkpoint",
                195 /* expected row count */,
                tblLocation -> {
                    CreateTableAndInsertData createTableAndInsertData =
                            new CreateTableAndInsertData();
                    createTableAndInsertData.createTableWithSampleData(tblLocation);
                    createTableAndInsertData.insertWithOptionalCheckpoint(tblLocation);
                });
    }

    private void verifyRowCount(String testName, int expectedRowCount, CheckedFunction<String> test)
            throws Exception {
        System.out.println("\n========== TEST START: " + testName + " ==============");
        try {
            String tblLocation = tmpLocation();

            test.apply(tblLocation);

            SingleThreadedTableReader reader = new SingleThreadedTableReader(tblLocation);
            // Select a large number of rows (1M), so that everything in the table is read.
            int actRowCount = reader.show(1_000_000, Optional.empty(), Optional.empty());
            if (actRowCount != expectedRowCount) {
                throw new RuntimeException(String.format(
                        "Test (%s) failed: expected row count = %s, actual row count = %s",
                        testName, expectedRowCount, actRowCount));
            }
        } finally {
            System.out.println("========== TEST END: " + testName + " ==============\n");
        }
    }

    private String tmpLocation() throws IOException {
        return Files.createTempDirectory("delta" + UUID.randomUUID()).toString();
    }

    interface CheckedFunction<T> {
        void apply(T tblLocation) throws Exception;
    }
}
