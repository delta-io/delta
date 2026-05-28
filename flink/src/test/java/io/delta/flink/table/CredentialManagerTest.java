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

import io.delta.flink.Conf;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/** JUnit test suite for CredentialManager. */
class CredentialManagerTest {

  @Test
  void testGetAndAutoRefreshCredentials() throws InterruptedException {
    Supplier<Map<String, String>> supplier =
        new Supplier<Map<String, String>>() {
          private int callCount = 0;

          @Override
          public Map<String, String> get() {
            long currentTime = System.currentTimeMillis();
            long refreshInterval = Conf.getInstance().getCredentialsRefreshAheadInMs();
            Map<String, String> result = new HashMap<>();
            result.put("authKey", "authValue" + callCount);
            // Refresh after around 100 ms
            result.put(
                CredentialManager.CREDENTIAL_EXPIRATION_KEY,
                String.valueOf(currentTime + refreshInterval + 100));
            callCount++;
            return result;
          }
        };

    CredentialManager manager = new CredentialManager(supplier, () -> {});

    // Initial values
    Map<String, String> initialResult = manager.getCredentials();
    Thread.sleep(150);
    // Refreshed values
    Map<String, String> refreshedResult = manager.getCredentials();

    assertEquals("authValue0", initialResult.get("authKey"));
    assertEquals("authValue1", refreshedResult.get("authKey"));
  }
}
