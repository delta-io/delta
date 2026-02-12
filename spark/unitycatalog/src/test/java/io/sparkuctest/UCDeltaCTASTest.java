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
import java.util.UUID;
import org.apache.hadoop.fs.Path;

/** Simple CTAS (Create Table As Select) test for lightweight debugging. */
public class UCDeltaCTASTest extends UCDeltaTableIntegrationBaseTest {

  @TestAllTableTypes
  public void testBasicCTAS(TableType tableType) {
    UnityCatalogInfo uc = unityCatalogInfo();
    String tableName = "ctas_basic" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    String fullTableName = uc.catalogName() + "." + uc.schemaName() + "." + tableName;

    String ctasSql;
    if (tableType == TableType.EXTERNAL) {
      Path tableLocation =
          new Path(uc.baseTableLocation(), "temp-" + UUID.randomUUID() + "/" + tableName);
      ctasSql =
          String.format(
              "CREATE TABLE %s USING DELTA LOCATION '%s' AS SELECT 1 AS id, 'hello' AS name",
              fullTableName, tableLocation);
    } else {
      ctasSql =
          String.format(
              "CREATE TABLE %s USING DELTA "
                  + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported') "
                  + "AS SELECT 1 AS id, 'hello' AS name",
              fullTableName);
    }

    try {
      sql(ctasSql);
      check(fullTableName, List.of(List.of("1", "hello")));
    } finally {
      sql("DROP TABLE IF EXISTS %s", fullTableName);
    }
  }
}
