package io.delta.flink

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration

class DummyHttp(returns: Map[String, String]) {

  val wireMockServer = new WireMockServer(
    WireMockConfiguration.options().dynamicPort())
  wireMockServer.start()
  configureFor("localhost", wireMockServer.port())

  returns.foreach { case (url, content) =>
    wireMockServer.stubFor(
      any(urlPathMatching(url))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(content)))
  }

  def port(): Int = wireMockServer.port()

}

object DummyHttp {
  def forUC(tablePath: String): DummyHttp =
    new DummyHttp(
      Map(
        "/api/2.1/unity-catalog/tables" -> "{}", // For write
        "/api/2.1/unity-catalog/tables/.*" -> // For read
          s"{\"storage_location\": \"$tablePath\", \"table_id\": \"dummy_id\"}",
        "/api/2.1/unity-catalog/temporary-table-credentials" -> "{}",
        "/api/2.1/unity-catalog/delta/preview/commits" ->
          "{\"commits\": [], \"latest_table_version\": 1230}"))
}
