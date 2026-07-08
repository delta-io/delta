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

import io.unitycatalog.server.service.credential.azure.ADLSStorageConfig;
import io.unitycatalog.server.service.credential.azure.AzureCredential;
import io.unitycatalog.server.service.credential.azure.AzureCredentialGenerator;

/**
 * Server-side Azure (ABFS) credential generator, paired with {@link AbfsCredentialFileSystem}. It
 * stamps the SAS token with the mint timestamp so the filesystem can parse it back out. See {@link
 * TimeBasedCredGenerator} for how the clock and duration are selected.
 *
 * <p>Loaded reflectively by the UC server via the {@code (ADLSStorageConfig)} constructor.
 */
public class AbfsTimeBasedCredGenerator extends TimeBasedCredGenerator<AzureCredential>
    implements AzureCredentialGenerator {
  public AbfsTimeBasedCredGenerator(ADLSStorageConfig ignore) {}

  @Override
  protected AzureCredential newTimeBasedCred(long mintMillis, long durMillis) {
    return AzureCredential.builder()
        .sasToken("sasToken-" + mintMillis)
        .expirationTimeInEpochMillis(mintMillis + durMillis)
        .build();
  }
}
