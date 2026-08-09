/*
 *  Copyright (2026) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.inttest;

import java.net.URI;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.spark.sql.SparkSession;

public class FlinkSqlIntTest extends IntTestBase {

  public FlinkSqlIntTest(SparkSession spark, URI catalogEndpoint, String catalogToken) {
    super(spark, catalogEndpoint, catalogToken);
  }

  @IntTest
  void testLoadUcTable() throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    TableEnvironment tEnv = TableEnvironment.create(settings);
    int numRecords = 5000;
    tEnv.executeSql(
        "CREATE TEMPORARY TABLE src (\n"
            + "id INT,\n"
            + "name STRING\n"
            + ") WITH (\n"
            + "'connector' = 'datagen',\n"
            + "'rows-per-second' = '1000',\n"
            + "'fields.id.kind' = 'sequence',\n"
            + "'fields.id.start' = '1',\n"
            + "'fields.id.end' = '"
            + numRecords
            + "'\n"
            + ")");

    tEnv.executeSql(
        String.format(
            "CREATE CATALOG main WITH (\n"
                + "   'type' = 'unitycatalog',\n"
                + "   'endpoint' = '%s',\n"
                + "   'token' = '%s'\n"
                + ")",
            catalogEndpoint.toString(), catalogToken));

    tEnv.executeSql("INSERT INTO main.hao.flink_stress SELECT id, name FROM src").await();
  }
}
