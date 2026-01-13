/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.spark.internal.v2.snapshot.unitycatalog;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for {@link UCTableInfo}. */
class UCTableInfoTest {

  @Test
  void testConstructor() {
    // Use distinctive values that would fail if implementation had hardcoded defaults
    String tableId = "uc_tbl_7f3a9b2c-e8d1-4f6a";
    String tablePath = "abfss://container@acct.dfs.core.windows.net/delta/v2";
    String ucUri = "https://uc-server.example.net/api/2.1/uc";
    String ucToken = "dapi_Kx9mN$2pQr#7vWz";

    Map<String, String> authConfig = new HashMap<>();
    authConfig.put("type", "static");
    authConfig.put("token", ucToken);

    UCTableInfo info = new UCTableInfo(tableId, tablePath, ucUri, authConfig);

    assertEquals(tableId, info.getTableId(), "Table ID should be stored correctly");
    assertEquals(tablePath, info.getTablePath(), "Table path should be stored correctly");
    assertEquals(ucUri, info.getUcUri(), "UC URI should be stored correctly");

    Map<String, String> ret = info.getAuthConfig();
    assertEquals("static", ret.get("type"), "Type should be static");
    assertEquals(ucToken, ret.get("token"), "UC token should be stored correctly in configMap");
  }
}
