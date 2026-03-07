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

package io.delta.flink;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import dev.failsafe.function.CheckedConsumer;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MockHttp {

  public static void withMock(MockHttp mock, CheckedConsumer<MockHttp> body) {
    try {
      body.accept(mock);
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
    mock.wireMockServer.stop();
  }

  public static MockHttp forNewUCTable(String tableId, String tablePath) {
    Map<String, String> stubs = new HashMap<>();
    stubs.put(
        "/api/2.1/unity-catalog/staging-tables",
        String.format("{\"id\": \"%s\", \"staging_location\":\"%s\"}", tableId, tablePath));
    stubs.put("/api/2.1/unity-catalog/tables", "{}"); // For write
    stubs.put("/api/2.1/unity-catalog/temporary-table-credentials", "{}");
    stubs.put(
        "/api/2.1/unity-catalog/delta/preview/commits",
        "{\"commits\": [], \"latest_table_version\": 1230}");

    Map<String, String> errors =
        Map.of(
            "/api/2.1/unity-catalog/tables/.*", // For read
            "{\"error_code\": \"TABLE_DOES_NOT_EXIST\", \"message\": \"\"}");

    return new MockHttp(stubs, errors);
  }

  public static MockHttp forExistingUCTable(String tablePath) {
    Map<String, String> stubs = new HashMap<>();
    stubs.put("/api/2.1/unity-catalog/staging-tables", "{}"); // For write
    stubs.put("/api/2.1/unity-catalog/tables", "{}"); // For write
    stubs.put(
        "/api/2.1/unity-catalog/tables/.*", // For read
        String.format("{\"storage_location\": \"%s\", \"table_id\": \"dummy_id\"}", tablePath));
    stubs.put("/api/2.1/unity-catalog/temporary-table-credentials", "{}");
    stubs.put(
        "/api/2.1/unity-catalog/delta/preview/commits",
        "{\"commits\": [], \"latest_table_version\": 1230}");
    return new MockHttp(stubs, Map.of());
  }

  private final WireMockServer wireMockServer;

  public MockHttp(Map<String, String> returns, Map<String, String> errors) {
    this.wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
    this.wireMockServer.start();
    configureFor("localhost", this.wireMockServer.port());

    returns.forEach(
        (url, content) ->
            this.wireMockServer.stubFor(
                any(urlPathMatching(url))
                    .willReturn(
                        aResponse()
                            .withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody(content))));
    errors.forEach(
        (url, content) ->
            this.wireMockServer.stubFor(
                any(urlPathMatching(url))
                    .willReturn(
                        aResponse()
                            .withStatus(404)
                            .withHeader("Content-Type", "application/json")
                            .withBody(content))));
  }

  public URI uri() {
    return URI.create("http://localhost:" + port());
  }

  public int port() {
    return this.wireMockServer.port();
  }
}
