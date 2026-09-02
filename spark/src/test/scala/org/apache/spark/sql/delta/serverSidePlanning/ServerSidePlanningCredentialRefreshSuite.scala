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

package org.apache.spark.sql.delta.serverSidePlanning

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicReference

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class ServerSidePlanningCredentialRefreshSuite extends QueryTest with SharedSparkSession {

  private var server: HttpServer = _

  override def afterEach(): Unit = {
    if (server != null) {
      server.stop(0)
      server = null
    }
    super.afterEach()
  }

  test("reader Hadoop configuration uses plan-scoped renewable credentials") {
    val observedQuery = new AtomicReference[String]()
    val observedAuthorization = new AtomicReference[String]()
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/v1/ns/t/credentials", exchange => {
      observedQuery.set(exchange.getRequestURI.getRawQuery)
      observedAuthorization.set(exchange.getRequestHeaders.getFirst("Authorization"))
      respond(exchange, 200,
        """{"storage-credentials":[{"prefix":"s3://bucket/table","config":{
          |"s3.access-key-id":"ak","s3.secret-access-key":"sk",
          |"s3.session-token":"st",
          |"s3.session-token-expires-at-ms":"4102444800000"}}]}""".stripMargin)
    })
    server.start()

    val baseUri = s"http://127.0.0.1:${server.getAddress.getPort}"
    val refresh = ScanPlanCredentialRefresh(
      catalogUri = baseUri,
      credentialsEndpoint = s"$baseUri/v1/ns/t/credentials",
      planId = "plan-1",
      authConfig = Map("type" -> "static", "token" -> "token-1"),
      storageSchemes = Seq("s3"))
    val schema = StructType.fromDDL("id INT")

    val factory = new ServerSidePlannedFilePartitionReaderFactory(
      spark, schema, schema, credentials = None, credentialRefresh = Some(refresh))
    val conf = factory.hadoopConf.value

    assert(observedQuery.get() == "planId=plan-1")
    assert(observedAuthorization.get() == "Bearer token-1")
    assert(conf.get("fs.unitycatalog.credentials.type") == "iceberg-plan")
    assert(conf.get("fs.unitycatalog.iceberg.credentials.endpoint") ==
      s"$baseUri/v1/ns/t/credentials")
    assert(conf.get("fs.unitycatalog.iceberg.plan.id") == "plan-1")
    assert(conf.get("fs.s3a.aws.credentials.provider").contains("AwsVendedTokenProvider"))
  }

  test("reader keeps static credential fallback without plan refresh context") {
    val staticCredentials = new ScanPlanStorageCredentials {
      override def configure(conf: org.apache.hadoop.conf.Configuration): Unit = {
        conf.set("fs.test.static.credential", "configured")
      }
    }
    val schema = StructType.fromDDL("id INT")

    val factory = new ServerSidePlannedFilePartitionReaderFactory(
      spark, schema, schema, credentials = Some(staticCredentials))

    assert(factory.hadoopConf.value.get("fs.test.static.credential") == "configured")
    assert(factory.hadoopConf.value.get("fs.unitycatalog.iceberg.plan.id") == null)
  }

  private def respond(exchange: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.set("Content-Type", "application/json")
    exchange.sendResponseHeaders(status, bytes.length)
    exchange.getResponseBody.write(bytes)
    exchange.close()
  }
}
