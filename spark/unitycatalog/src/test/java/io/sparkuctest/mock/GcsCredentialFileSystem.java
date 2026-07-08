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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import io.unitycatalog.hadoop.internal.auth.GcsVendedTokenProvider;

/** Resolves the connector's vended GCS access token and checks it is currently valid. */
public class GcsCredentialFileSystem extends CredentialTestFileSystem<AccessTokenProvider> {
  @Override
  protected String scheme() {
    return "gs";
  }

  @Override
  protected AccessTokenProvider createProvider() {
    String clazz = getConf().get("fs.gs.auth.access.token.provider");
    assertThat(clazz).isEqualTo(GcsVendedTokenProvider.class.getName());
    // setConf() validates the Hadoop conf, failing fast if required properties are missing.
    AccessTokenProvider provider = new GcsVendedTokenProvider();
    provider.setConf(getConf());
    return provider;
  }

  @Override
  protected long resolveCredentialMintMillis(AccessTokenProvider provider) {
    AccessTokenProvider.AccessToken token = provider.getAccessToken();
    assertThat(token).isNotNull();
    return mintMillisOf(token.getToken(), String.format("testing-renew://gs://%s#", BUCKET_NAME));
  }
}
