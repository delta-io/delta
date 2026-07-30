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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import java.time.Instant;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/** JUnit test suite for FixedGcsAccessTokenProvider. */
class FixedGcsAccessTokenProviderTest {

  @Test
  void getAccessTokenReturnsTokenWithExplicitExpiration() {
    Configuration conf = new Configuration();
    conf.set("fs.gs.auth.access.token", "test-token-123");
    conf.set("fs.gs.auth.access.token.expiration.ms", "1700000000000");

    FixedGcsAccessTokenProvider provider = new FixedGcsAccessTokenProvider();
    provider.setConf(conf);

    AccessTokenProvider.AccessToken accessToken = provider.getAccessToken();
    assertNotNull(accessToken);
    assertEquals("test-token-123", accessToken.getToken());
    assertEquals(Instant.ofEpochMilli(1700000000000L), accessToken.getExpirationTime());
  }

  @Test
  void getAccessTokenUsesFallbackWhenNoExpiration() {
    Configuration conf = new Configuration();
    conf.set("fs.gs.auth.access.token", "test-token-456");

    FixedGcsAccessTokenProvider provider = new FixedGcsAccessTokenProvider();
    provider.setConf(conf);

    long before = System.currentTimeMillis();
    AccessTokenProvider.AccessToken accessToken = provider.getAccessToken();
    long after = System.currentTimeMillis();

    assertNotNull(accessToken);
    assertEquals("test-token-456", accessToken.getToken());
    long expirationMs = accessToken.getExpirationTime().toEpochMilli();
    assertTrue(expirationMs >= before + 3600_000L);
    assertTrue(expirationMs <= after + 3600_000L);
  }

  @Test
  void getAccessTokenUsesFallbackOnInvalidExpiration() {
    Configuration conf = new Configuration();
    conf.set("fs.gs.auth.access.token", "test-token-789");
    conf.set("fs.gs.auth.access.token.expiration.ms", "not-a-number");

    FixedGcsAccessTokenProvider provider = new FixedGcsAccessTokenProvider();
    provider.setConf(conf);

    long before = System.currentTimeMillis();
    AccessTokenProvider.AccessToken accessToken = provider.getAccessToken();
    long after = System.currentTimeMillis();

    assertNotNull(accessToken);
    assertEquals("test-token-789", accessToken.getToken());
    long expirationMs = accessToken.getExpirationTime().toEpochMilli();
    assertTrue(expirationMs >= before + 3600_000L);
    assertTrue(expirationMs <= after + 3600_000L);
  }

  @Test
  void getAccessTokenThrowsWhenTokenMissing() {
    Configuration conf = new Configuration();

    FixedGcsAccessTokenProvider provider = new FixedGcsAccessTokenProvider();
    provider.setConf(conf);

    assertThrows(RuntimeException.class, provider::getAccessToken);
  }
}
