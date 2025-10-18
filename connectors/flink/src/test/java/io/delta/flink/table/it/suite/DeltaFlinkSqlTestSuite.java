/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.flink.table.it.suite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;
import static io.delta.flink.utils.DeltaTestUtils.buildCluster;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Integration tests that uses none Delta Source and Sink tables with Delta Catalog.
 */
public abstract class DeltaFlinkSqlTestSuite {

    private static final int PARALLELISM = 2;

    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private static final String DATAGEN_SOURCE_DDL = ""
        + "CREATE TABLE sourceTable ("
        + " col1 VARCHAR,"
        + " col2 VARCHAR,"
        + " col3 INT,"
        + " col4 AS col3 * 2"
        + ") WITH ("
        + "'connector' = 'datagen',"
        + "'rows-per-second' = '1',"
        + "'fields.col3.kind' = 'sequence',"
        + "'fields.col3.start' = '1',"
        + "'fields.col3.end' = '5'"
        + ")";

    private final MiniClusterWithClientResource miniClusterResource = buildCluster(PARALLELISM);

    public TableEnvironment tableEnv;

    @BeforeAll
    public static void beforeAll() throws IOException {
        TEMPORARY_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        TEMPORARY_FOLDER.delete();
    }

    @BeforeEach
    public void setUp() {
        try {
            miniClusterResource.before();
            tableEnv = StreamTableEnvironment.create(getTestStreamEnv());
            setupDeltaCatalog(tableEnv);
        } catch (Exception e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    @AfterEach
    public void afterEach() {
        miniClusterResource.after();
    }

    /**
     * Test that Delta Catalog and Delta Table Factory can support Flink SQL pipeline that does
     * not use Delta Table connector.
     * <p>
     * Tested Source - Sink connectors: datagen -> blackhole
     */
    @Test
    public void testPipelineWithoutDeltaTables_1() throws Exception {

        String sinkTableSql = "CREATE TABLE sinkTable ("
            + " col1 VARCHAR,"
            + " col2 VARCHAR,"
            + " col3 INT,"
            + " col4 INT"
            + ") WITH ("
            + "  'connector' = 'blackhole'"
            + ");";

        tableEnv.executeSql(DATAGEN_SOURCE_DDL);
        tableEnv.executeSql(sinkTableSql);

        String querySql = "INSERT INTO sinkTable SELECT * FROM sourceTable";
        TableResult result = tableEnv.executeSql(querySql);

        List<Row> results = new ArrayList<>();
        try (org.apache.flink.util.CloseableIterator<Row> collect = result.collect()) {
            collect.forEachRemaining(results::add);
        }

        assertThat(results).hasSize(1);
        assertThat(results.get(0).getKind()).isEqualTo(RowKind.INSERT);
    }

    /**
     * Test that Delta Catalog and Delta Table Factory can support Flink SQL pipeline that does
     * not use Delta Table connector.
     * <p>
     * Tested Source - Sink connectors: datagen -> filesystem
     */
    @Test
    public void testPipelineWithoutDeltaTables_2() throws Exception {

        String targetTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();

        String sinkTableSql = String.format(
            "CREATE TABLE sinkTable ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT,"
                + " col4 INT"
                + ") WITH ("
                + " 'connector' = 'filesystem',"
                + " 'path' = '%s',"
                + " 'auto-compaction' = 'false',"
                + " 'format' = 'parquet',"
                + " 'sink.parallelism' = '2'"
                + ")",
            targetTablePath);

        List<Row> sinkRows= executeSqlJob(DATAGEN_SOURCE_DDL, sinkTableSql);
        long uniqueValues = getUniqueValues(sinkRows);

        assertThat(sinkRows).hasSize(5);
        assertThat(uniqueValues).isEqualTo(5L);
    }

    /**
     * Test that Delta Catalog and Delta Table Factory can support Flink SQL pipeline that does
     * not use Delta Table connector.
     * <p>
     * Tested Source - Sink connectors: filesystem -> filesystem with partitions
     */
    @Test
    public void testPipelineWithoutDeltaTables_3() throws Exception {

        String sourceTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        String targetTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();

        DeltaTestUtils.initTestForTableApiTable(sourceTablePath);

        String sourceTableSql = String.format(""
                + "CREATE TABLE sourceTable ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + ") WITH ("
                + " 'connector' = 'filesystem',"
                + " 'path' = '%s',"
                + " 'format' = 'parquet'"
                + ")",
            sourceTablePath
        );

        String sinkTableSql = String.format(
            "CREATE TABLE sinkTable ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + ") "
                + "PARTITIONED BY (col1)"
                + "WITH ("
                + " 'connector' = 'filesystem',"
                + " 'path' = '%s',"
                + " 'auto-compaction' = 'false',"
                + " 'format' = 'parquet',"
                + " 'sink.parallelism' = '2'"
                + ")",
            targetTablePath);

        List<Row> sinkRows = executeSqlJob(sourceTableSql, sinkTableSql);
        long uniqueValues = getUniqueValues(sinkRows);

        assertThat(sinkRows).hasSize(1);
        assertThat(uniqueValues).isEqualTo(1L);
    }

    @Test
    public void testSelectDeltaTableAsTempTable() throws Exception {

        // GIVEN
        String sourceTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        DeltaTestUtils.initTestForTableApiTable(sourceTablePath);

        String sourceTableSql = String.format(
            "CREATE TABLE sourceTable ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + ") "
                + "WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            sourceTablePath);

        tableEnv.executeSql(sourceTableSql);

        String tempDeltaTable = "CREATE TEMPORARY TABLE sourceTable_tmp"
            + "  WITH  ("
            + " 'mode' = 'streaming'"
            + ")"
            + "  LIKE sourceTable;";

        tableEnv.executeSql(tempDeltaTable);

        // WHEN
        String selectSql = "SELECT * FROM sourceTable_tmp";

        // THEN
        ValidationException validationException =
            assertThrows(ValidationException.class, () -> tableEnv.executeSql(selectSql));

        assertThat(
            validationException.getCause().getMessage())
            .withFailMessage(
                "Using Flink Temporary tables should not be possible since those are always using"
                    + "Flink's default in-memory catalog.")
            .contains("Delta Table SQL/Table API was used without Delta Catalog.");
    }

    @Test
    public void testSelectViewFromDeltaTable() throws Exception {

        // GIVEN
        String sourceTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        DeltaTestUtils.initTestForTableApiTable(sourceTablePath);

        String sourceTableSql = String.format(
            "CREATE TABLE sourceTable ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + ") "
                + "WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            sourceTablePath);

        tableEnv.executeSql(sourceTableSql);

        String viewSql = "CREATE VIEW sourceTable_view AS "
            + "SELECT col1 from sourceTable";

        String temporaryViewSql = "CREATE TEMPORARY VIEW sourceTable_view_tmp AS "
            + "SELECT col1 from sourceTable";

        tableEnv.executeSql(viewSql);
        tableEnv.executeSql(temporaryViewSql);

        // WHEN
        String selectViewSql = "SELECT * FROM sourceTable_view";
        String selectViewTmpSql = "SELECT * FROM sourceTable_view_tmp";

        // THEN
        TableResult selectViewResult = tableEnv.executeSql(selectViewSql);
        TableResult selectTmpViewResult = tableEnv.executeSql(selectViewTmpSql);

        assertSelectResult(selectViewResult);
        assertSelectResult(selectTmpViewResult);
    }

    @Test
    public void testSelectWithClauseFromDeltaTable() throws Exception {

        // GIVEN
        String sourceTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        DeltaTestUtils.initTestForTableApiTable(sourceTablePath);

        String sourceTableSql = String.format(
            "CREATE TABLE sourceTable ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + ") "
                + "WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            sourceTablePath);

        tableEnv.executeSql(sourceTableSql);

        // WHEN
        String withSelect = "WITH sourceTable_with AS ("
            + "SELECT col1 FROM sourceTable"
            + ") "
            + "SELECT * FROM sourceTable_with";

        // THEN
        TableResult selectViewResult= tableEnv.executeSql(withSelect);

        assertSelectResult(selectViewResult);
    }

    public abstract void setupDeltaCatalog(TableEnvironment tableEnv);

    private void assertSelectResult(TableResult selectResult) throws Exception {
        List<Row> sourceRows = new ArrayList<>();
        try (CloseableIterator<Row> collect = selectResult.collect()) {
            collect.forEachRemaining(sourceRows::add);
        }

        assertThat(sourceRows).hasSize(1);
    }

    private StreamExecutionEnvironment getTestStreamEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE);
        return env;
    }

    private List<Row> executeSqlJob(String sourceTableSql, String sinkTableSql) {
        try {
            tableEnv.executeSql(sourceTableSql);
            tableEnv.executeSql(sinkTableSql);

            String insertSql = "INSERT INTO sinkTable SELECT * FROM sourceTable";
            tableEnv.executeSql(insertSql).await(10, TimeUnit.SECONDS);

            String selectSql = "SELECT * FROM sinkTable";
            TableResult selectResult = tableEnv.executeSql(selectSql);

            List<Row> sinkRows = new ArrayList<>();
            try (org.apache.flink.util.CloseableIterator<Row> collect = selectResult.collect()) {
                collect.forEachRemaining(sinkRows::add);
                return sinkRows;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private long getUniqueValues(List<Row> sinkRows) {
        return sinkRows.stream()
            .map((Function<Row, Integer>) row -> row.getFieldAs("col3"))
            .distinct().count();
    }
}
