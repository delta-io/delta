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

import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.spark.sql.delta.sources.DeltaSQLConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class UCManagedMetadataOnlyCreateV2Test extends UCDeltaTableIntegrationBaseTest {
  private String tableToCleanUp;

  @AfterEach
  public void cleanUpTable() {
    if (tableToCleanUp != null) {
      try {
        sql("DROP TABLE IF EXISTS %s", tableToCleanUp);
      } catch (Exception ignored) {
        // Ignore cleanup failures so the test can report the real failure.
      }
      tableToCleanUp = null;
    }
  }

  @Test
  public void testManagedMetadataOnlyCreateUsesCommittedSnapshotPublication() throws Exception {
    tableToCleanUp =
        String.format(
            "%s.%s.metadata_only_create_%s",
            unityCatalogInfo().catalogName(),
            unityCatalogInfo().schemaName(),
            UUID.randomUUID().toString().replace('-', '_'));

    withV2CreateMode(
        "AUTO",
        () -> {
          sql(
              "CREATE TABLE %s (i INT, s STRING) USING DELTA "
                  + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported', 'Foo'='Bar') "
                  + "COMMENT 'metadata-only create'",
              tableToCleanUp);

          check(tableToCleanUp, List.of());

          TablesApi tablesApi = new TablesApi(unityCatalogInfo().createApiClient());
          TableInfo tableInfo = tablesApi.getTable(tableToCleanUp, false, false);

          assertThat(tableInfo.getDataSourceFormat()).isEqualTo(DataSourceFormat.DELTA);
          assertThat(tableInfo.getTableType().name()).isEqualTo("MANAGED");
          // TODO: Kernel's CreateTableTransactionBuilder does not yet support withDescription(),
          // so the COMMENT clause is not propagated to the Delta log or UC table metadata.
          // assertThat(tableInfo.getComment()).isEqualTo("metadata-only create");

          List<String> columnNames =
              tableInfo.getColumns().stream().map(ColumnInfo::getName).collect(Collectors.toList());
          assertThat(columnNames).containsExactlyInAnyOrder("i", "s");

          assertThat(tableInfo.getProperties())
              .containsEntry("Foo", "Bar")
              .containsEntry("delta.feature.catalogManaged", "supported")
              .containsEntry("io.unitycatalog.tableId", tableInfo.getTableId())
              .containsEntry("delta.lastUpdateVersion", "0")
              .containsKey("delta.lastCommitTimestamp");
        });
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private void withV2CreateMode(String mode, ThrowingRunnable runnable) throws Exception {
    scala.Option<String> original = spark().conf().getOption(DeltaSQLConf.V2_ENABLE_MODE().key());
    spark().conf().set(DeltaSQLConf.V2_ENABLE_MODE().key(), mode);
    try {
      runnable.run();
    } finally {
      if (original.isDefined()) {
        spark().conf().set(DeltaSQLConf.V2_ENABLE_MODE().key(), original.get());
      } else {
        spark().conf().unset(DeltaSQLConf.V2_ENABLE_MODE().key());
      }
    }
  }
}
