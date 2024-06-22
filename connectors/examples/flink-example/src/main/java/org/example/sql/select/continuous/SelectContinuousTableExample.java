package org.example.sql.select.continuous;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.utils.Utils;
import org.utils.job.sql.ContinuousSqlSourceExampleBase;

/**
 * This is an example of executing a continuous SELECT query on Delta Table using Flink SQL.
 */
public class SelectContinuousTableExample extends ContinuousSqlSourceExampleBase {

    private static final String TABLE_PATH =
        Utils.resolveExampleTableAbsolutePath("data/source_table_no_partitions");

    public static void main(String[] args) throws Exception {
        new SelectContinuousTableExample().run(TABLE_PATH);
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

        // A SQL query that fetches all columns from sourceTable.
        // This query runs in continuous mode.
        return tableEnv.sqlQuery("SELECT * FROM sourceTable /*+ OPTIONS('mode' = 'streaming') */");
    }
}
