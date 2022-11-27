/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.tables;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.delta.DeltaLog;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.*;

import org.apache.spark.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.spark.sql.delta.DeltaSQLCommandJavaTest;

import static org.apache.spark.sql.types.DataTypes.*;

public class JavaDeltaTableBuilderSuite implements DeltaSQLCommandJavaTest {

    private transient SparkSession spark;
    private transient String input;


    @Before
    public void setUp() {
        // Trigger static initializer of TestData
        spark = buildSparkSession();
    }

    @After
    public void tearDown() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    private DeltaTable buildTable(DeltaTableBuilder builder) {
        return builder.addColumn("c1", "int")
            .addColumn("c2", IntegerType)
            .addColumn("c3", "string", false)
            .addColumn("c4", StringType, true)
            .addColumn(DeltaTable.columnBuilder(spark, "c5")
                .dataType("bigint")
                .comment("foo")
                .nullable(false)
                .build()
            )
            .addColumn(DeltaTable.columnBuilder(spark, "c6")
                .dataType(LongType)
                .generatedAlwaysAs("c5 + 10")
                .build()
            ).execute();
    }

    private DeltaTable createTable(boolean ifNotExists, String tableName) {
        DeltaTableBuilder builder;
        if (ifNotExists) {
            builder = DeltaTable.createIfNotExists();
        } else {
            builder = DeltaTable.create();
        }
        if (tableName.startsWith("delta.`")) {
            tableName = tableName.substring("delta.`".length());
            String location = tableName.substring(0, tableName.length() - 1);
            builder = builder.location(location);
            DeltaLog.forTable(spark, location).clearCache();
        } else {
            builder = builder.tableName(tableName);
            DeltaLog.forTable(spark, new Path(tableName)).clearCache();
        }
        return buildTable(builder);
    }

    private DeltaTable replaceTable(boolean orCreate, String tableName) {
        DeltaTableBuilder builder;
        if (orCreate) {
            builder = DeltaTable.createOrReplace();
        } else {
            builder = DeltaTable.replace();
        }
        if (tableName.startsWith("delta.`")) {
            tableName = tableName.substring("delta.`".length());
            String location = tableName.substring(0, tableName.length() - 1);
            builder = builder.location(location);
        } else {
            builder = builder.tableName(tableName);
        }
        return buildTable(builder);
    }

    private void verifyGeneratedColumn(String tableName, DeltaTable deltaTable) {
        String cmd = String.format("INSERT INTO %s (c1, c2, c3, c4, c5, c6) %s", tableName,
            "VALUES (1, 2, 'a', 'c', 1, 11)");
        spark.sql(cmd);
        Map<String, String> set = new HashMap<String, String>() {{
            put("c5", "10");
        }};
        deltaTable.updateExpr("c6 = 11", set);
        assert(deltaTable.toDF().select("c6").collectAsList().get(0).getLong(0) == 20);
    }

    @Test
    public void testCreateTable() {
        try {
            // Test creating DeltaTable by name
            DeltaTable table = createTable(false, "deltaTable");
            verifyGeneratedColumn("deltaTable", table);
        } finally {
            spark.sql("DROP TABLE IF EXISTS deltaTable");
        }
        // Test creating DeltaTable by path.
        String input = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "input")
            .toString();
        DeltaTable table2 = createTable(false, String.format("delta.`%s`", input));
        verifyGeneratedColumn(String.format("delta.`%s`", input), table2);
    }

    @Test
    public void testCreateTableIfNotExists() {
        // Ignore table creation if already exsits.
        List<String> data = Arrays.asList("hello", "world");
        Dataset<Row> dataDF = spark.createDataset(data, Encoders.STRING()).toDF();
        try {
            // Test creating DeltaTable by name - not exists.
            DeltaTable table = createTable(true, "deltaTable");
            verifyGeneratedColumn("deltaTable", table);

            dataDF.write().format("delta").mode("overwrite").saveAsTable("deltaTable2");

            // Table 2 should be the old table saved by path.
            DeltaTable table2 = DeltaTable.createIfNotExists().tableName("deltaTable2")
                .addColumn("value", "string")
                .execute();
            QueryTest$.MODULE$.checkAnswer(table2.toDF(), dataDF.collectAsList());
        } finally {
            spark.sql("DROP TABLE IF EXISTS deltaTable");
            spark.sql("DROP TABLE IF EXISTS deltaTable2");
        }
        // Test creating DeltaTable by path.
        String input = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "input")
            .toString();
        dataDF.write().format("delta").mode("overwrite").save(input);
        DeltaTable table = createTable(true, String.format("delta.`%s`", input));
        QueryTest$.MODULE$.checkAnswer(table.toDF(), dataDF.collectAsList());
    }

    @Test
    public void testCreateTableWithExistingSchema() {
        try {
            // Test create table with an existing schema.
            List<String> data = Arrays.asList("hello", "world");
            Dataset<Row> dataDF = spark.createDataset(data, Encoders.STRING()).toDF();

            DeltaLog.forTable(spark, new Path("deltaTable")).clearCache();
            DeltaTable table = DeltaTable.create().tableName("deltaTable")
                .addColumns(dataDF.schema())
                .execute();
            dataDF.write().format("delta").mode("append").saveAsTable("deltaTable");

            QueryTest$.MODULE$.checkAnswer(table.toDF(), dataDF.collectAsList());
        } finally {
            spark.sql("DROP TABLE IF EXISTS deltaTable");
        }
    }

    @Test
    public void testReplaceTable() {
        try {
            // create a table first
            spark.sql("CREATE TABLE deltaTable (col1 int) USING delta");
            // Test replacing DeltaTable by name
            DeltaTable table = replaceTable(false, "deltaTable");
            verifyGeneratedColumn("deltaTable", table);
        } finally {
            spark.sql("DROP TABLE IF EXISTS deltaTable");
        }
        String input = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "input")
            .toString();
        List<String> data = Arrays.asList("hello", "world");
        Dataset<Row> dataDF = spark.createDataset(data, Encoders.STRING()).toDF();
        dataDF.write().format("delta").mode("overwrite").save(input);
        DeltaTable table = replaceTable(false, String.format("delta.`%s`", input));
        verifyGeneratedColumn(String.format("delta.`%s`", input), table);
    }

    @Test
    public void testCreateOrReplaceTable() {
        try {
            // Test creating DeltaTable by name if table to be replaced does not exist.
            DeltaTable table = replaceTable(true, "deltaTable");
            verifyGeneratedColumn("deltaTable", table);
        } finally {
            spark.sql("DROP TABLE IF EXISTS deltaTable");
        }
    }
}
