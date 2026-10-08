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
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.kernel.unitycatalog.UCTableIdentifier;
import io.delta.storage.commit.uccommitcoordinator.UCConfigUtils;
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
    UCTableIdentifier tableIdentifier =
        new UCTableIdentifier("prod_catalog", "analytics", "page_views");
    String ucUri = "https://uc-server.example.net/api/2.1/uc";
    String ucToken = "dapi_Kx9mN$2pQr#7vWz";

    Map<String, String> authConfig = new HashMap<>();
    authConfig.put("type", "static");
    authConfig.put("token", ucToken);

    UCTableInfo info = new UCTableInfo(tableId, tablePath, tableIdentifier, ucUri, authConfig);

    assertEquals(tableId, info.getTableId(), "Table ID should be stored correctly");
    assertEquals(tablePath, info.getTablePath(), "Table path should be stored correctly");
    assertEquals(tableIdentifier, info.getTableIdentifier(), "Identifier should be stored");
    assertEquals(ucUri, info.getUcUri(), "UC URI should be stored correctly");

    Map<String, String> ret = info.getAuthConfig();
    assertEquals("static", ret.get("type"), "Type should be static");
    assertEquals(ucToken, ret.get("token"), "UC token should be stored correctly in configMap");
  }

  @Test
  void testConstructorRequiresTableIdentifier() {
    Map<String, String> authConfig = new HashMap<>();
    authConfig.put("token", "fake-token");

    NullPointerException ex =
        assertThrows(
            NullPointerException.class,
            () ->
                new UCTableInfo(
                    "uc_tbl_123",
                    "s3://bucket/table",
                    null,
                    "https://uc-server.example.net/api/2.1/uc",
                    authConfig));

    assertEquals("tableIdentifier is null", ex.getMessage());
  }

  @Test
  void testToUcConfigForwardsOptInFlags() {
    // The opt-in flags must reach `toUcConfig`'s output verbatim; otherwise
    // `UCTokenBasedRestClientFactory.createUCClient` falls back to the legacy client
    // even when the catalog has opted into the new Delta API.
    Map<String, String> authConfig = new HashMap<>();
    authConfig.put("type", "static");
    authConfig.put("token", "tok");

    Map<String, String> optInFlags = new HashMap<>();
    optInFlags.put(UCConfigUtils.DELTA_REST_API_ENABLED_KEY, "true");
    optInFlags.put(UCConfigUtils.RENEW_CREDENTIAL_ENABLED_KEY, "true");
    optInFlags.put(UCConfigUtils.CRED_SCOPED_FS_ENABLED_KEY, "false");

    UCTableInfo info =
        new UCTableInfo(
            "uc_tbl_1",
            "s3://bucket/tbl",
            new UCTableIdentifier("cat", "sch", "tbl"),
            "https://uc.example.net",
            authConfig,
            optInFlags);

    Map<String, String> ucConfig = info.toUcConfig();
    assertEquals("https://uc.example.net", ucConfig.get("uri"));
    assertEquals("static", ucConfig.get("auth.type"));
    assertEquals("tok", ucConfig.get("auth.token"));
    assertEquals("true", ucConfig.get(UCConfigUtils.DELTA_REST_API_ENABLED_KEY));
    assertEquals("true", ucConfig.get(UCConfigUtils.RENEW_CREDENTIAL_ENABLED_KEY));
    assertEquals("false", ucConfig.get(UCConfigUtils.CRED_SCOPED_FS_ENABLED_KEY));
  }

  @Test
  void testLegacyConstructorDefaultsOptInFlagsToEmpty() {
    // Callers that don't pass optInFlags get the legacy behavior: no flags emitted into
    // `toUcConfig`. The factory falls back to the default UC client, preserving prior
    // semantics for code paths that haven't migrated.
    Map<String, String> authConfig = new HashMap<>();
    authConfig.put("token", "tok");

    UCTableInfo info =
        new UCTableInfo(
            "uc_tbl_1",
            "s3://bucket/tbl",
            new UCTableIdentifier("cat", "sch", "tbl"),
            "https://uc.example.net",
            authConfig);

    assertEquals(true, info.getOptInFlags().isEmpty(), "Default optInFlags should be empty");
    Map<String, String> ucConfig = info.toUcConfig();
    assertEquals(null, ucConfig.get(UCConfigUtils.DELTA_REST_API_ENABLED_KEY));
    assertEquals(null, ucConfig.get(UCConfigUtils.RENEW_CREDENTIAL_ENABLED_KEY));
    assertEquals(null, ucConfig.get(UCConfigUtils.CRED_SCOPED_FS_ENABLED_KEY));
  }
}
