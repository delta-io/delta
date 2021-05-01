/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

import io.delta.tables.DeltaTable;

import org.apache.spark.sql.*;
import org.apache.spark.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.sql.test.TestSparkSession;

public class MergeIntoJavaSuite implements Serializable {
    private transient TestSparkSession spark;
    private transient String tempPath;

    @Before
    public void setUp() {
        spark = new TestSparkSession();
        tempPath = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "spark").toString();
    }

    @After
    public void tearDown() {
        spark.stop();
        spark = null;
    }

    @Test
    public void checkBasicApi() {
        Dataset<Row> targetTable = createKVDataSet(
            Arrays.asList(tuple2(1, 10), tuple2(2, 20)), "key1", "value1");
        targetTable.write().format("delta").save(tempPath);

        Dataset<Row> sourceTable = createKVDataSet(
            Arrays.asList(tuple2(1, 100), tuple2(3, 30)), "key2", "value2");

        DeltaTable target = DeltaTable.forPath(spark, tempPath);
        Map<String, String> updateMap = new HashMap<String, String>() {{
            put("key1", "key2");
            put("value1", "value2");
        }};
        Map<String, String> insertMap = new HashMap<String, String>() {{
            put("key1", "key2");
            put("value1", "value2");
        }};
        target.merge(sourceTable, "key1 = key2")
            .whenMatched()
            .updateExpr(updateMap)
            .whenNotMatched()
            .insertExpr(insertMap)
            .execute();

        List<Row> expectedAnswer = createKVDataSet(
            Arrays.asList(tuple2(1, 100), tuple2(2, 20), tuple2(3, 30))).collectAsList();

        QueryTest$.MODULE$.checkAnswer(target.toDF(), expectedAnswer);
    }

    @Test
    public void checkExtendedApi() {
        Dataset<Row> targetTable = createKVDataSet(
            Arrays.asList(tuple2(1, 10), tuple2(2, 20)), "key1", "value1");
        targetTable.write().format("delta").save(tempPath);

        Dataset<Row> sourceTable = createKVDataSet(
            Arrays.asList(tuple2(1, 100), tuple2(3, 30)), "key2", "value2");

        DeltaTable target = DeltaTable.forPath(spark, tempPath);
        Map<String, String> updateMap = new HashMap<String, String>() {{
            put("key1", "key2");
            put("value1", "value2");
        }};
        Map<String, String> insertMap = new HashMap<String, String>() {{
            put("key1", "key2");
            put("value1", "value2");
        }};
        target.merge(sourceTable, "key1 = key2")
            .whenMatched("key1 = 4").delete()
            .whenMatched("key2 = 1")
            .updateExpr(updateMap)
            .whenNotMatched("key2 = 3")
            .insertExpr(insertMap)
            .execute();

        List<Row> expectedAnswer = createKVDataSet(
            Arrays.asList(tuple2(1, 100), tuple2(2, 20), tuple2(3, 30))).collectAsList();

        QueryTest$.MODULE$.checkAnswer(target.toDF(), expectedAnswer);
    }

    @Test
    public void checkExtendedApiWithColumn() {
        Dataset<Row> targetTable = createKVDataSet(
            Arrays.asList(tuple2(1, 10), tuple2(2, 20), tuple2(4, 40)), "key1", "value1");
        targetTable.write().format("delta").save(tempPath);

        Dataset<Row> sourceTable = createKVDataSet(
            Arrays.asList(tuple2(1, 100), tuple2(3, 30), tuple2(4, 41)), "key2", "value2");

        DeltaTable target = DeltaTable.forPath(spark, tempPath);
        Map<String, Column> updateMap = new HashMap<String, Column>() {{
            put("key1", functions.col("key2"));
            put("value1", functions.col("value2"));
        }};
        Map<String, Column> insertMap = new HashMap<String, Column>() {{
            put("key1", functions.col("key2"));
            put("value1", functions.col("value2"));
        }};
        target.merge(sourceTable, functions.expr("key1 = key2"))
            .whenMatched(functions.expr("key1 = 4")).delete()
            .whenMatched(functions.expr("key2 = 1"))
            .update(updateMap)
            .whenNotMatched(functions.expr("key2 = 3"))
            .insert(insertMap)
            .execute();

        List<Row> expectedAnswer = createKVDataSet(
            Arrays.asList(tuple2(1, 100), tuple2(2, 20), tuple2(3, 30))).collectAsList();

        QueryTest$.MODULE$.checkAnswer(target.toDF(), expectedAnswer);
    }

    @Test
    public void checkUpdateAllAndInsertAll() {
        Dataset<Row> targetTable = createKVDataSet(Arrays.asList(
            tuple2(1, 10), tuple2(2, 20), tuple2(4, 40), tuple2(5, 50)), "key", "value");
        targetTable.write().format("delta").save(tempPath);

        Dataset<Row> sourceTable = createKVDataSet(Arrays.asList(
            tuple2(1, 100), tuple2(3, 30), tuple2(4, 41), tuple2(5, 51), tuple2(6, 60)),
            "key", "value");

        DeltaTable target = DeltaTable.forPath(spark, tempPath);
        target.as("t").merge(sourceTable.as("s"), functions.expr("t.key = s.key"))
            .whenMatched().updateAll()
            .whenNotMatched().insertAll()
            .execute();

        List<Row> expectedAnswer = createKVDataSet(Arrays.asList(tuple2(1, 100), tuple2(2, 20),
            tuple2(3, 30), tuple2(4, 41), tuple2(5, 51), tuple2(6, 60))).collectAsList();

        QueryTest$.MODULE$.checkAnswer(target.toDF(), expectedAnswer);
    }

    private Dataset<Row> createKVDataSet(
        List<Tuple2<Integer, Integer>> data, String keyName, String valueName) {
        Encoder<Tuple2<Integer, Integer>> encoder = Encoders.tuple(Encoders.INT(), Encoders.INT());
        return spark.createDataset(data, encoder).toDF(keyName, valueName);
    }

    private Dataset<Row> createKVDataSet(List<Tuple2<Integer, Integer>> data) {
        Encoder<Tuple2<Integer, Integer>> encoder = Encoders.tuple(Encoders.INT(), Encoders.INT());
        return spark.createDataset(data, encoder).toDF();
    }

    private <T1, T2> Tuple2<T1, T2> tuple2(T1 t1, T2 t2) {
        return new Tuple2<>(t1, t2);
    }
}
