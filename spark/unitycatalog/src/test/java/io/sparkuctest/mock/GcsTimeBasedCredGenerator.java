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

package io.sparkuctest.mock;

import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialGenerator;
import java.time.Instant;
import java.util.Date;

/**
 * Server-side GCS credential generator, paired with {@link GcsCredentialFileSystem}. It stamps the
 * access token value with the mint timestamp so the filesystem can parse it back out. See {@link
 * TimeBasedCredGenerator} for how the clock and duration are selected.
 *
 * <p>Loaded reflectively by the UC server via a no-arg constructor.
 */
public class GcsTimeBasedCredGenerator extends TimeBasedCredGenerator<AccessToken>
    implements GcpCredentialGenerator {
  @Override
  protected AccessToken newTimeBasedCred(long mintMillis, long durMillis) {
    Instant expiration = Instant.ofEpochMilli(mintMillis + durMillis);
    return AccessToken.newBuilder()
        .setTokenValue(
            String.format(
                "testing-renew://gs://%s#%d", CredentialTestFileSystem.BUCKET_NAME, mintMillis))
        .setExpirationTime(Date.from(expiration))
        .build();
  }
}
