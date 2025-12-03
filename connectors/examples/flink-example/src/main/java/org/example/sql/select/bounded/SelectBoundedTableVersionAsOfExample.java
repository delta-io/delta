package org.example.sql.select.bounded;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.utils.Utils;
import org.utils.job.sql.BoundedSqlSourceExampleBase;

/**
 * This is an example of executing a bounded SELECT query on Delta Table using Flink SQL
 * that will read Delta table from version specified by `versionAsOf` option.
 */
public class SelectBoundedTableVersionAsOfExample extends BoundedSqlSourceExampleBase {

    private static final String TABLE_PATH =
        Utils.resolveExampleTableAbsolutePath("data/source_table_no_partitions");

    public static void main(String[] args) throws Exception {
        new SelectBoundedTableVersionAsOfExample().run(TABLE_PATH);
    }

    @Override
    protected Table runSqlJob(String tablePath, StreamTableEnvironment tableEnv) {

        // setup Delta Catalog
        tableEnv.executeSql("CREATE CATALOG myDeltaCatalog WITH ('type' = 'delta-catalog')");
        tableEnv.executeSql("USE CATALOG myDeltaCatalog");

        // SQL definition for Delta Table where we will insert rows.
        tableEnv.executeSql(String.format(""
                + "CREATE TABLE sourceTable ("
                + "f1 STRING,"
                + "f2 STRING,"
                + "f3 INT"
                + ") WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            tablePath)
        );

        // A SQL query that fetches all columns from sourceTable starting from Delta version 1.
        // This query runs in batch mode which is a default mode for SQL queries on Delta Table.
        return tableEnv.sqlQuery("SELECT * FROM sourceTable /*+ OPTIONS('versionAsOf' = '1') */");
    }
}
