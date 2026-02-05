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

import io.delta.kernel.Snapshot;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.TableInfo;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder$;
import org.junit.jupiter.api.Test;

/** Minimal customer-journey test for CCv2 create via Unity Catalog. */
public class UCCatalogManagedCreateJourneyTest extends UCDeltaTableIntegrationBaseTest {

  /**
   * Customer-journey test for STRICT UC-managed CREATE TABLE using:
   *
   * <ul>
   *   <li>Kernel metadata-only create (writes `_delta_log/00000000000000000000.json`)
   *   <li>Unity Catalog registration (table is queryable via the UC catalog)
   *   <li>Kernel UC snapshot load (table is readable immediately after CREATE)
   * </ul>
   *
   * This catches regressions where catalog-managed tables cannot be loaded due to missing UC commit
   * history immediately after table creation.
   */
  @Test
  public void testCreateManagedTableWithCcV2Strict() throws Exception {
    spark().conf().set("spark.databricks.delta.v2.enableMode", "STRICT");
    String tableName = "ccv2_create_" + UUID.randomUUID().toString().replace("-", "");

    withNewTable(
        tableName,
        "id INT",
        TableType.MANAGED,
        fullTableName -> {
          TablesApi tablesApi = new TablesApi(unityCatalogInfo().createApiClient());
          TableInfo tableInfo = tablesApi.getTable(fullTableName, false, false);

          String location = tableInfo.getStorageLocation();
          assertThat(location).isNotNull().isNotEmpty();

          String ucTableId = tableInfo.getTableId();
          assertThat(ucTableId).isNotNull().isNotEmpty();

          Path logFile = new Path(new Path(location), "_delta_log/00000000000000000000.json");
          assertThat(logFile.getFileSystem(spark().sessionState().newHadoopConf()).exists(logFile))
              .isTrue();

          List<List<String>> props =
              sql(
                  "SHOW TBLPROPERTIES %s ('%s')",
                  fullTableName, UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
          assertThat(props).hasSize(1);
          assertThat(props.get(0).get(1)).isEqualTo(ucTableId);

          // Table is readable immediately after CREATE.
          assertThat(spark().sql("SELECT * FROM " + fullTableName).collectAsList()).isEmpty();

          // Kernel UC snapshot load should also succeed immediately after CREATE.
          DefaultEngine engine = DefaultEngine.create(spark().sessionState().newHadoopConf());
          String resolvedPath = engine.getFileSystemClient().resolvePath(location);

          UCCommitCoordinatorClient coordinator =
              (UCCommitCoordinatorClient)
                  UCCommitCoordinatorBuilder$.MODULE$.buildForCatalog(
                      spark(), unityCatalogInfo().catalogName());
          try (UCClient ucClient = coordinator.ucClient) {
            Snapshot snapshot =
                new UCCatalogManagedClient(ucClient)
                    .loadSnapshot(
                        engine, ucTableId, resolvedPath, Optional.empty(), Optional.empty());
            SnapshotImpl snapshotImpl = (SnapshotImpl) snapshot;
            assertThat(snapshot.getVersion()).isEqualTo(0L);
            assertThat(snapshot.getTableProperties().get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY))
                .isEqualTo(ucTableId);
            assertThat(snapshotImpl.getProtocol().getReaderFeatures())
                .contains(TableFeatures.CATALOG_MANAGED_RW_FEATURE.featureName());
            assertThat(snapshotImpl.getProtocol().getWriterFeatures())
                .contains(TableFeatures.CATALOG_MANAGED_RW_FEATURE.featureName());
          }
        });
  }
}
