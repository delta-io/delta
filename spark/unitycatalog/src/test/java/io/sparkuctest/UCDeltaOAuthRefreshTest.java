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

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Exercises OAuth access-token renewal. With a token lifetime well under the connector's ~30s
 * renewal lead, the OAuthTokenProvider re-runs the client-credentials grant on essentially every
 * authenticated call, so a handful of table operations must drive more than one token issuance at
 * the broker.
 *
 * <p>Scope and limitation (token expiration is a deferred problem): this verifies only that the
 * connector RE-FETCHES a token (the issued-token count grows). It is open-loop: it cannot verify
 * that a non-renewed token would actually be rejected, nor any server-side expiry enforcement,
 * because UC-internal tokens currently carry no exp claim and UC never expires them. The lifetime
 * that triggers renewal here is the broker's advertised expires_in, not anything UC issues. So this
 * proves the client renews, not that renewal is required or that expiry is enforced.
 */
public class UCDeltaOAuthRefreshTest extends UCDeltaTableIntegrationBaseTest {

  /** Cap the advertised lifetime at 1s so the token is always due for renewal. */
  @Override
  protected long oauthTokenMaxLifetimeSeconds() {
    return 1;
  }

  @Test
  public void renewsTokenAcrossOperations() throws Exception {
    int before = oauthIssuedTokenCount();
    // Captured mid-run (inside the lambda) to show the count keeps growing across operations.
    int[] afterFirstOp = {0};

    withNewTable(
        "oauth_refresh_tbl",
        "a INT, id BIGINT",
        TableType.MANAGED,
        fullName -> {
          sql("INSERT INTO %s VALUES (1, 10)", fullName);
          afterFirstOp[0] = oauthIssuedTokenCount();
          sql("SELECT * FROM %s", fullName);
          sql("INSERT INTO %s VALUES (2, 20)", fullName);
          check(fullName, List.of(row("1", "10"), row("2", "20")));
        });

    int after = oauthIssuedTokenCount();
    // With a 1s lifetime cap the connector is always past its ~30s renewal lead, so it re-runs the
    // client-credentials grant on each authenticated call. Proving *renewal* (not a one-time issue)
    // requires the count to grow as the connector first authenticates AND to keep growing across
    // later operations -- i.e. the token is genuinely re-acquired, not fetched once and cached.
    assertThat(afterFirstOp[0])
        .as("token issued once the connector authenticates")
        .isGreaterThan(before);
    assertThat(after)
        .as("token re-issued across later operations (renewal exercised)")
        .isGreaterThan(afterFirstOp[0]);
  }
}
