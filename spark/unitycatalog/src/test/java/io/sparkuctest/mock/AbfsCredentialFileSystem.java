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

import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.AbfsVendedTokenProvider;

/** Resolves the connector's vended ABFS SAS token and checks it is currently valid. */
public class AbfsCredentialFileSystem extends CredentialTestFileSystem<AbfsVendedTokenProvider> {
  @Override
  protected String scheme() {
    return "abfs";
  }

  @Override
  protected AbfsVendedTokenProvider createProvider() {
    String clazz = getConf().get(UCHadoopConfConstants.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE);
    assertThat(clazz).isEqualTo(AbfsVendedTokenProvider.class.getName());
    AbfsVendedTokenProvider provider = new AbfsVendedTokenProvider();
    provider.initialize(getConf(), "testAccount");
    return provider;
  }

  @Override
  protected long resolveCredentialMintMillis(AbfsVendedTokenProvider provider) {
    String sasToken = provider.getSASToken("testAccount", "testFs", "testPath", "testOperation");
    return mintMillisOf(sasToken, "sasToken-");
  }
}
