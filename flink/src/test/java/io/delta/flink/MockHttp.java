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
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import dev.failsafe.function.CheckedConsumer;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MockHttp {

  private static final String DELTA_TABLES_PATH =
      "/api/2.1/unity-catalog/delta/v1/catalogs/[^/]+/schemas/[^/]+/tables/[^/]+$";
  private static final String DELTA_CREDENTIALS_PATH =
      "/api/2.1/unity-catalog/delta/v1/catalogs/[^/]+/schemas/[^/]+/tables/[^/]+/credentials$";
  private static final String DELTA_CREATE_TABLE_PATH =
      "/api/2.1/unity-catalog/delta/v1/catalogs/[^/]+/schemas/[^/]+/tables$";
  private static final String DELTA_STAGING_TABLE_PATH =
      "/api/2.1/unity-catalog/delta/v1/catalogs/[^/]+/schemas/[^/]+/staging-tables$";
  private static final String DELTA_STAGING_CREDENTIALS_PATH =
      "/api/2.1/unity-catalog/delta/v1/staging-tables/[^/]+/credentials$";
  private static final String DELTA_METRICS_PATH =
      "/api/2.1/unity-catalog/delta/v1/catalogs/[^/]+/schemas/[^/]+/tables/[^/]+/metrics$";

  private static String loadTableJson(String tableId, String tablePath) {
    return String.format(
        "{\"metadata\": {\"table-uuid\": \"%s\", \"location\": \"%s\", "
            + "\"table-type\": \"MANAGED\", \"columns\": {\"type\": \"struct\", "
            + "\"fields\": []}}, \"commits\": [], \"latest-table-version\": 0}",
        tableId, tablePath);
  }

  public static void withMock(MockHttp mock, CheckedConsumer<MockHttp> body) {
    try {
      body.accept(mock);
    } catch (Throwable e) {
      throw new RuntimeException(e);
    } finally {
      mock.wireMockServer.stop();
    }
  }

  public static MockHttp forNewUCTable(String tableId, String tablePath) {
    Map<String, String> stubs = new HashMap<>();
    stubs.put(DELTA_CREDENTIALS_PATH, "{}");
    stubs.put(DELTA_STAGING_CREDENTIALS_PATH, "{}");
    stubs.put(DELTA_METRICS_PATH, "{}");
    MockHttp mock = new MockHttp(stubs, Map.of());

    String scenario = "table-lifecycle";
    mock.wireMockServer.stubFor(
        get(urlPathMatching(DELTA_TABLES_PATH))
            .inScenario(scenario)
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(notFound()));
    mock.wireMockServer.stubFor(
        post(urlPathMatching(DELTA_STAGING_TABLE_PATH))
            .inScenario(scenario)
            .willReturn(
                okJson(
                    String.format(
                        "{\"table-id\": \"%s\", \"table-type\": \"MANAGED\", \"location\": \"%s\", "
                            + "\"required-protocol\": {\"min-reader-version\": 3, "
                            + "\"min-writer-version\": 7, \"reader-features\": [\"deletionVectors\"], "
                            + "\"writer-features\": [\"deletionVectors\"]}, "
                            + "\"required-properties\": {\"delta.enableDeletionVectors\": \"true\", "
                            + "\"io.unitycatalog.tableId\": \"%s\"}}",
                        tableId, tablePath, tableId))));
    mock.wireMockServer.stubFor(
        post(urlPathMatching(DELTA_CREATE_TABLE_PATH))
            .inScenario(scenario)
            .willReturn(okJson(loadTableJson(tableId, tablePath)))
            .willSetStateTo("created"));
    mock.wireMockServer.stubFor(
        get(urlPathMatching(DELTA_TABLES_PATH))
            .inScenario(scenario)
            .whenScenarioStateIs("created")
            .willReturn(okJson(loadTableJson(tableId, tablePath))));
    mock.wireMockServer.stubFor(post(urlPathMatching(DELTA_TABLES_PATH)).willReturn(okJson("{}")));
    return mock;
  }

  public static MockHttp forExistingUCTable(String tableId, String tablePath) {
    return forExistingUCTable(tableId, tablePath, "{}");
  }

  public static MockHttp forExistingUCTable(
      String tableId, String tablePath, String credentialsResponse) {
    Map<String, String> stubs = new HashMap<>();
    stubs.put("/api/2.1/unity-catalog/staging-tables", "{}"); // For write
    stubs.put("/api/2.1/unity-catalog/tables", "{}"); // For write
    stubs.put(DELTA_TABLES_PATH, loadTableJson(tableId, tablePath));
    stubs.put("/api/2.1/unity-catalog/temporary-table-credentials", "{}");
    stubs.put(DELTA_CREDENTIALS_PATH, credentialsResponse);
    stubs.put(
        "/api/2.1/unity-catalog/delta/preview/commits",
        "{\"commits\": [], \"latest_table_version\": 1230}");
    stubs.put(DELTA_METRICS_PATH, "{}");
    return new MockHttp(stubs, Map.of());
  }

  public static MockHttp forMissingUCTable() {
    return new MockHttp(Map.of(), Map.of(DELTA_TABLES_PATH, "{\"error\":\"not found\"}"));
  }

  public static MockHttp forUnsupportedUCTable() {
    MockHttp mock = new MockHttp(Map.of(), Map.of());
    mock.wireMockServer.stubFor(
        get(urlPathMatching(DELTA_TABLES_PATH))
            .willReturn(
                aResponse()
                    .withStatus(400)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        "{\"error\":{\"code\":400,"
                            + "\"type\":\"UnsupportedTableFormatException\","
                            + "\"message\":\"Table is not a Delta table\"}}")));
    return mock;
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

  public void verifyCredentialRequests(int count) {
    wireMockServer.verify(count, getRequestedFor(urlPathMatching(DELTA_CREDENTIALS_PATH)));
  }

  public void verifyStagingCredentialRequests(int count) {
    wireMockServer.verify(count, getRequestedFor(urlPathMatching(DELTA_STAGING_CREDENTIALS_PATH)));
  }

  public void verifyPostRequest(String path) {
    this.wireMockServer.verify(1, postRequestedFor(urlPathEqualTo(path)));
  }
}
