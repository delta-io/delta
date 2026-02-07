/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.sparkuctest;

import static org.assertj.core.api.Assertions.assertThat;

import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

/** Minimal customer-journey test for CCv2 create via Unity Catalog. */
public class UCCatalogManagedCreateJourneyTest extends UCDeltaTableIntegrationBaseTest {

  // Creates a UC-managed table in STRICT mode and verifies UC metadata + delta log creation.
  @Test
  public void testCreateManagedTableWithCcV2Strict() throws Exception {
    spark().conf().set("spark.databricks.delta.v2.enableMode", "STRICT");
    String tableName = "ccv2_create_" + UUID.randomUUID().toString().replace("-", "");

    withNewTable(
        tableName,
        "id INT",
        TableType.MANAGED,
        fullTableName -> {
          Dataset<Row> detail = spark().sql("DESCRIBE DETAIL " + fullTableName);
          Row row = detail.collectAsList().get(0);
          String location = row.getAs("location");
          assertThat(location).isNotNull();

          Path logFile = new Path(new Path(location), "_delta_log/00000000000000000000.json");
          assertThat(logFile.getFileSystem(spark().sessionState().newHadoopConf()).exists(logFile))
              .isTrue();

          List<List<String>> props =
              sql(
                  "SHOW TBLPROPERTIES %s ('%s')",
                  fullTableName, UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
          assertThat(props).hasSize(1);
          assertThat(props.get(0).get(1)).isNotEmpty();
        });
  }
}
