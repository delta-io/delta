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

import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.delta.catalog.UCDeltaCatalogClientImpl;
import org.junit.jupiter.api.Test;

/**
 * Integration test for the non-Delta fallback path in {@code UCDeltaCatalogClientImpl}.
 *
 * <p>When {@code deltaRestApi.enabled=true}, every {@code loadTable} call first asks the Delta REST
 * API. If the target table is not in Delta format, UC returns {@code
 * UnsupportedTableFormatException}; the client must then fall back to the legacy {@code
 * TableCatalog} delegate so the non-Delta table is still readable.
 *
 * <p>This class disables the class-level "Delta REST API served at least one load" assertion (see
 * {@link #expectDeltaRestApiSuccessAtClassLevel()}) because its tests intentionally exercise only
 * the fallback path, which does not bump {@code SUCCESSFUL_DELTA_REST_API_LOADS}. CI sharding also
 * makes a same-file "sanity" Delta test unreliable: methods can be distributed across shards, so
 * each shard's @AfterAll runs without a guarantee of seeing both methods.
 */
public class UCDeltaTableNonDeltaFallbackTest extends UCDeltaTableIntegrationBaseTest {

  @Override
  protected boolean expectDeltaRestApiSuccessAtClassLevel() {
    return false;
  }

  @Test
  public void testLoadNonDeltaParquetExternalTableFallsBackToLegacyCatalog() throws Exception {
    String tableName = "non_delta_parquet_fallback";
    String fullTableName = fullTableName(tableName);
    withTempDir(
        (Path dir) -> {
          Path tablePath = new Path(dir, tableName);
          sql("DROP TABLE IF EXISTS %s", fullTableName);
          try {
            // Create a non-Delta EXTERNAL Parquet table. UC accepts external non-Delta tables;
            // managed non-Delta tables are rejected upstream, so EXTERNAL is the only shape
            // that reaches the loadTable fallback path.
            sql(
                "CREATE TABLE %s (id INT, name STRING) USING parquet LOCATION '%s'",
                fullTableName, tablePath);
            sql("INSERT INTO %s VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')", fullTableName);

            long invocationsBefore = UCDeltaCatalogClientImpl.LOAD_TABLE_INVOCATIONS().get();
            long successesBefore = UCDeltaCatalogClientImpl.SUCCESSFUL_DELTA_REST_API_LOADS().get();

            // The Delta REST API path runs first: ucClient.loadTable -> UC server returns
            // UnsupportedTableFormatException (table isn't Delta-format) -> the catch handler
            // calls fallbackLoadTable(ident) which is super.loadTable from AbstractDeltaCatalog
            // (i.e. the legacy DelegatingCatalogExtension delegate). The SELECT below succeeds
            // only if that fallback hands back a usable V1 table for the Parquet data.
            List<List<String>> rows = sql("SELECT id, name FROM %s ORDER BY id", fullTableName);
            check(
                rows, List.of(List.of("1", "alpha"), List.of("2", "beta"), List.of("3", "gamma")));

            // Counter delta: loadTable was invoked (catch handler ran) but the Delta REST API
            // did NOT successfully serve the load. A future regression that silently returned
            // a (wrong) Delta table from the REST path would bump the success counter and fail
            // this assertion, even though the row data check above would also still pass.
            long invocationsAfter = UCDeltaCatalogClientImpl.LOAD_TABLE_INVOCATIONS().get();
            long successesAfter = UCDeltaCatalogClientImpl.SUCCESSFUL_DELTA_REST_API_LOADS().get();
            if (invocationsAfter <= invocationsBefore) {
              throw new AssertionError(
                  "Expected LOAD_TABLE_INVOCATIONS to increase during the SELECT, but it did not"
                      + " (before="
                      + invocationsBefore
                      + ", after="
                      + invocationsAfter
                      + ")");
            }
            if (successesAfter != successesBefore) {
              throw new AssertionError(
                  "Expected SUCCESSFUL_DELTA_REST_API_LOADS to be unchanged (fallback path took"
                      + " over), but it changed (before="
                      + successesBefore
                      + ", after="
                      + successesAfter
                      + ")");
            }
          } finally {
            sql("DROP TABLE IF EXISTS %s", fullTableName);
          }
        });
  }
}
