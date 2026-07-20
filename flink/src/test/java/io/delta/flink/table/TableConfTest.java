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

package io.delta.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** JUnit test suite for {@link TableConf}. */
class TableConfTest {

  @Test
  void testEngineConfIncludesAllCustomerProvidedFsKeys() {
    Map<String, String> raw = new HashMap<>();
    raw.put("fs.s3a.access.key", "AKIA");
    raw.put("fs.s3a.secret.key", "secret");
    raw.put("fs.azure.account.key.acct.dfs.core.windows.net", "azkey");
    // Non-fs keys must not leak into engineConf.
    raw.put("delta.feature.v2Checkpoint", "supported");
    raw.put("io.unitycatalog.tableId", "uuid");
    raw.put("checkpoint.frequency", "0.5");

    Map<String, String> engineConf = new TableConf(raw).engineConf();

    assertEquals(3, engineConf.size());
    assertEquals("AKIA", engineConf.get("fs.s3a.access.key"));
    assertEquals("secret", engineConf.get("fs.s3a.secret.key"));
    assertEquals("azkey", engineConf.get("fs.azure.account.key.acct.dfs.core.windows.net"));
    assertFalse(engineConf.containsKey("delta.feature.v2Checkpoint"));
    assertFalse(engineConf.containsKey("io.unitycatalog.tableId"));
    assertFalse(engineConf.containsKey("checkpoint.frequency"));
  }

  @Test
  void testEngineConfEmptyWhenNoFsKeys() {
    Map<String, String> raw = new HashMap<>();
    raw.put("delta.appendOnly", "true");
    raw.put("checkpoint.frequency", "1.0");

    assertTrue(new TableConf(raw).engineConf().isEmpty());
  }

  @Test
  void testEngineConfPicksUpKeysAddedViaUpdate() {
    TableConf conf = new TableConf(new HashMap<>());
    assertTrue(conf.engineConf().isEmpty());

    conf.update(Map.of("fs.gs.project.id", "my-project"));

    assertEquals(Map.of("fs.gs.project.id", "my-project"), conf.engineConf());
  }
}
