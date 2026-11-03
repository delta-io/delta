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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

/**
 * Smoke test for the static-token auth path. The rest of the integration suite defaults to OAUTH
 * (server-validating), so this single test keeps the static-token path -- a local server with
 * authorization disabled and the connector sending a static bearer -- covered end to end.
 */
@DisabledIf(
    value = "isUCRemoteConfigured",
    disabledReason =
        "authMode()=STATIC only controls the local server; remote auth is selected by env vars, "
            + "so this cannot force the static-token path remotely.")
public class UCDeltaStaticTokenTest extends UCDeltaTableIntegrationBaseTest {

  @Override
  protected AuthMode authMode() {
    return AuthMode.STATIC;
  }

  @Test
  public void readsAndWritesWithStaticToken() throws Exception {
    withNewTable(
        "static_token_tbl",
        "a INT, id BIGINT",
        TableType.MANAGED,
        fullName -> {
          sql("INSERT INTO %s VALUES (1, 10)", fullName);
          check(fullName, List.of(row("1", "10")));
        });
  }
}
