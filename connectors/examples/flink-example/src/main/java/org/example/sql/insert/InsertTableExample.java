package org.example.sql.insert;

import java.util.UUID;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.utils.Utils;
import org.utils.job.sql.SqlSinkExampleBase;

/**
 * This is an example of executing a INSERT query on Delta Table using Flink SQL.
 */
public class InsertTableExample extends SqlSinkExampleBase {

    static String TABLE_PATH = Utils.resolveExampleTableAbsolutePath(
        "example_table_" + UUID.randomUUID().toString().split("-")[0]);

    public static void main(String[] args)
        throws Exception {
        new InsertTableExample().run(TABLE_PATH);
    }

    @Override
    protected Table runSqlJob(String tablePath, StreamTableEnvironment tableEnv) {

        // setup Delta Catalog
        tableEnv.executeSql("CREATE CATALOG myDeltaCatalog WITH ('type' = 'delta-catalog')");
        tableEnv.executeSql("USE CATALOG myDeltaCatalog");

        // SQL definition for Delta Table where we will insert rows.
        tableEnv.executeSql(String.format(""
                + "CREATE TABLE sinkTable ("
                + "f1 STRING,"
                + "f2 STRING,"
                + "f3 INT"
                + ") WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            tablePath)
        );

        // A SQL query that inserts three rows (three columns per row) into sinkTable.
        tableEnv.executeSql(""
            + "INSERT INTO sinkTable VALUES "
            + "('a', 'b', 1),"
            + "('c', 'd', 2),"
            + "('e', 'f', 3)"
        );
        return null;
    }
}
