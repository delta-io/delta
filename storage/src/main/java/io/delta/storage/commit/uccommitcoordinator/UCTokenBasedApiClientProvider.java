/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.storage.commit.uccommitcoordinator;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.retry.JitterDelayRetryPolicy;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

abstract class UCTokenBasedApiClientProvider {
  private ApiClient apiClient;

  protected UCTokenBasedApiClientProvider(
      String baseUri,
      TokenProvider tokenProvider,
      Map<String, String> appVersions) {
    this.apiClient = buildApiClient(baseUri, tokenProvider, appVersions);
  }

  protected UCTokenBasedApiClientProvider(
      String baseUri,
      TokenProvider tokenProvider,
      Map<String, String> appVersions,
      String catalog) {
    // The catalog is consumed by subclasses that probe catalog-scoped Delta API support.
    this(baseUri, tokenProvider, appVersions);
  }

  private static ApiClient buildApiClient(
      String baseUri,
      TokenProvider tokenProvider,
      Map<String, String> appVersions) {
    Objects.requireNonNull(baseUri, "baseUri must not be null");
    Objects.requireNonNull(tokenProvider, "tokenProvider must not be null");
    Objects.requireNonNull(appVersions, "appVersions must not be null");

    ApiClientBuilder builder = ApiClientBuilder.create()
        .uri(baseUri)
        .tokenProvider(tokenProvider)
        .retryPolicy(JitterDelayRetryPolicy.builder().build());

    appVersions.forEach((name, version) -> {
      if (version != null) {
        builder.addAppVersion(name, version);
      }
    });

    return builder.build();
  }

  protected ApiClient getApiClient() {
    return apiClient;
  }

  public void close() throws IOException {
    apiClient = null;
  }
}
