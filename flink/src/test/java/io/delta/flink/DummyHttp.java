package io.delta.flink;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.util.HashMap;
import java.util.Map;

public class DummyHttp {

  private final WireMockServer wireMockServer;

  public DummyHttp(Map<String, String> returns) {
    this.wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
    this.wireMockServer.start();
    configureFor("localhost", this.wireMockServer.port());

    returns.forEach(
        (url, content) -> {
          this.wireMockServer.stubFor(
              any(urlPathMatching(url))
                  .willReturn(
                      aResponse()
                          .withStatus(200)
                          .withHeader("Content-Type", "application/json")
                          .withBody(content)));
        });
  }

  public int port() {
    return this.wireMockServer.port();
  }

  public static DummyHttp forUC(String tablePath) {
    Map<String, String> stubs = new HashMap<>();
    stubs.put("/api/2.1/unity-catalog/tables", "{}"); // For write
    stubs.put(
        "/api/2.1/unity-catalog/tables/.*", // For read
        String.format("{\"storage_location\": \"%s\", \"table_id\": \"dummy_id\"}", tablePath));
    stubs.put("/api/2.1/unity-catalog/temporary-table-credentials", "{}");
    stubs.put(
        "/api/2.1/unity-catalog/delta/preview/commits",
        "{\"commits\": [], \"latest_table_version\": 1230}");

    return new DummyHttp(stubs);
  }
}
