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
}
