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

package io.delta.storage.unitycatalog.hadoop

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.unitycatalog.client.auth.TokenProvider
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.{
  CredentialOperation,
  StorageCredential,
  StorageCredentialConfig
}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

import io.delta.storage.unitycatalog.hadoop.{
  CredPropsUtil,
  UCDeltaRestCatalogApiCredentialConf}
import io.delta.storage.unitycatalog.hadoop.fs.{CredScopedFileSystem, CredScopedFs}
import io.delta.storage.unitycatalog.hadoop.credentials.{
  AbfsVendedTokenProvider,
  AwsVendedTokenProvider,
  GcsVendedTokenProvider
}

class UCDeltaRestCatalogApiCredentialPropsUtilSuite extends AnyFunSuite {
  private val TableId = "table-id"
  private val TableCatalog = "uc"
  private val TableSchema = "default"
  private val TableName = "tbl"
  private val TableLocation = "s3://bucket/table"

  test("creates static S3 credential properties") {
    val props = CredPropsUtil.createTableCredProps(
      false,
      false,
      Map.empty[String, String].asJava,
      "s3a",
      "https://uc.example",
      tokenProvider(),
      TableId,
      TableCatalog,
      TableSchema,
      TableName,
      TableLocation,
      awsCredentials()).asScala.toMap

    assert(props === Map(
      "fs.s3a.path.style.access" -> "true",
      "fs.s3.impl.disable.cache" -> "true",
      "fs.s3a.impl.disable.cache" -> "true",
      "fs.s3a.access.key" -> "ak",
      "fs.s3a.secret.key" -> "sk",
      "fs.s3a.session.token" -> "st"))
  }

  test("creates static Azure and GCS credential properties") {
    val azureProps = CredPropsUtil.createTableCredProps(
      false,
      false,
      Map.empty[String, String].asJava,
      "abfss",
      "https://uc.example",
      tokenProvider(),
      TableId,
      TableCatalog,
      TableSchema,
      TableName,
      "abfss://container/path/table",
      azureCredentials()).asScala.toMap
    val gcsProps = CredPropsUtil.createTableCredProps(
      false,
      false,
      Map.empty[String, String].asJava,
      "gs",
      "https://uc.example",
      tokenProvider(),
      TableId,
      TableCatalog,
      TableSchema,
      TableName,
      "gs://bucket/table",
      gcsCredentials()).asScala.toMap

    assert(azureProps === Map(
      "fs.azure.account.auth.type" -> "SAS",
      "fs.azure.account.hns.enabled" -> "true",
      "fs.abfs.impl.disable.cache" -> "true",
      "fs.abfss.impl.disable.cache" -> "true",
      "fs.azure.sas.fixed.token" -> "sas"))
    assert(gcsProps === Map(
      "fs.gs.create.items.conflict.check.enable" -> "true",
      "fs.gs.impl.disable.cache" -> "true",
      "fs.gs.auth.access.token.credential" -> "token",
      "fs.gs.auth.access.token.expiration" -> Long.MaxValue.toString))
  }

  test("creates renewable table credential properties") {
    val props = CredPropsUtil.createTableCredProps(
      true,
      false,
      Map.empty[String, String].asJava,
      "s3",
      "https://uc.example",
      tokenProvider(),
      TableId,
      TableCatalog,
      TableSchema,
      TableName,
      TableLocation,
      awsCredentials(expirationTimeMs = 123L)).asScala.toMap

    assert(props(UCDeltaRestCatalogApiCredentialConf.S3A_CREDENTIALS_PROVIDER) ===
      classOf[AwsVendedTokenProvider].getName)
    assert(props(UCDeltaRestCatalogApiCredentialConf.UC_URI_KEY) === "https://uc.example")
    assert(props(UCDeltaRestCatalogApiCredentialConf.UC_AUTH_PREFIX + "type") === "static")
    assert(props(UCDeltaRestCatalogApiCredentialConf.UC_AUTH_PREFIX + "token") === "token")
    assert(props(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_KEY) ===
      UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_TABLE_VALUE)
    assert(props(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_ID_KEY) === TableId)
    assert(props(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_CATALOG_KEY) === TableCatalog)
    assert(props(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_SCHEMA_KEY) === TableSchema)
    assert(props(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_NAME_KEY) === TableName)
    assert(props(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_LOCATION_KEY) === TableLocation)
    assert(props(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_OPERATION_KEY) === "READ_WRITE")
    assert(props(UCDeltaRestCatalogApiCredentialConf.S3A_INIT_ACCESS_KEY) === "ak")
    assert(props(UCDeltaRestCatalogApiCredentialConf.S3A_INIT_SECRET_KEY) === "sk")
    assert(props(UCDeltaRestCatalogApiCredentialConf.S3A_INIT_SESSION_TOKEN) === "st")
    assert(props(UCDeltaRestCatalogApiCredentialConf.S3A_INIT_CRED_EXPIRED_TIME) === "123")
    assert(props.contains(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_UID_KEY))
  }

  test("renews table credentials through UC Delta Rest Catalog API") {
    var sawDeltaCredentialRequest = false
    var sawLegacyCredentialRequest = false
    var deltaCredentialRequestQuery: String = null

    withHttpServer { server =>
      server.createContext("/", exchange => {
        exchange.getRequestURI.getPath match {
          case "/api/2.1/unity-catalog/delta/v1/catalogs/uc/schemas/default/tables/tbl" |
              "/api/2.1/unity-catalog/delta/v1/catalogs/uc/schemas/default/tables/tbl/" =>
            sendJson(exchange, 500, "{}")
          case "/api/2.1/unity-catalog/delta/v1/catalogs/uc/schemas/default/tables/tbl/credentials" =>
            sawDeltaCredentialRequest = true
            deltaCredentialRequestQuery = exchange.getRequestURI.getQuery
            sendJson(exchange, 200, s3CredentialsResponseJson)
          case "/api/2.1/unity-catalog/temporary-table-credentials" =>
            sawLegacyCredentialRequest = true
            sendJson(exchange, 500, "{}")
          case path =>
            sendJson(exchange, 404, s"""{"unexpected_uri":"${exchange.getRequestURI}"}""")
        }
      })

      val ucUri = s"http://127.0.0.1:${server.getAddress.getPort}"
      val props = CredPropsUtil.createTableCredProps(
        true,
        false,
        Map.empty[String, String].asJava,
        "s3",
        ucUri,
        tokenProvider(),
        TableId,
        TableCatalog,
        TableSchema,
        TableName,
        TableLocation,
        awsCredentials(expirationTimeMs = 1L)).asScala.toMap
      val conf = new Configuration(false)
      props.foreach { case (key, value) => conf.set(key, value) }

      val credentials = new AwsVendedTokenProvider(conf).resolveCredentials()

      assert(credentials.accessKeyId() === "renewed-ak")
      assert(sawDeltaCredentialRequest)
      assert(deltaCredentialRequestQuery.contains("operation=READ_WRITE"))
      assert(!sawLegacyCredentialRequest)
    }
  }

  test("creates renewable path credential properties") {
    val azureProps = CredPropsUtil.createPathCredProps(
      true,
      false,
      Map.empty[String, String].asJava,
      "abfs",
      "https://uc.example",
      tokenProvider(),
      "abfs://container/path/table",
      azureCredentials(operation = CredentialOperation.READ, expirationTimeMs = 123L))
      .asScala.toMap
    val gcsProps = CredPropsUtil.createPathCredProps(
      true,
      false,
      Map.empty[String, String].asJava,
      "gs",
      "https://uc.example",
      tokenProvider(),
      "gs://bucket/table",
      gcsCredentials(operation = CredentialOperation.READ, expirationTimeMs = 456L))
      .asScala.toMap

    assert(azureProps(UCDeltaRestCatalogApiCredentialConf.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE) ===
      classOf[AbfsVendedTokenProvider].getName)
    assert(azureProps(UCDeltaRestCatalogApiCredentialConf.AZURE_INIT_SAS_TOKEN) === "sas")
    assert(
      azureProps(UCDeltaRestCatalogApiCredentialConf.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME) === "123")
    assert(
      azureProps(UCDeltaRestCatalogApiCredentialConf.UC_PATH_OPERATION_KEY) ===
        "PATH_READ")

    assert(gcsProps("fs.gs.auth.type") === "ACCESS_TOKEN_PROVIDER")
    assert(gcsProps("fs.gs.auth.access.token.provider") ===
      classOf[GcsVendedTokenProvider].getName)
    assert(gcsProps(UCDeltaRestCatalogApiCredentialConf.GCS_INIT_OAUTH_TOKEN) === "token")
    assert(
      gcsProps(UCDeltaRestCatalogApiCredentialConf.GCS_INIT_OAUTH_TOKEN_EXPIRATION_TIME) === "456")
    assert(gcsProps(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_KEY) ===
      UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_PATH_VALUE)
    assert(gcsProps(UCDeltaRestCatalogApiCredentialConf.UC_PATH_KEY) === "gs://bucket/table")
  }

  test("adds credential-scoped filesystem overrides when enabled") {
    val props = CredPropsUtil.createTableCredProps(
      true,
      true,
      Map("fs.s3a.impl" -> "com.example.CustomS3A").asJava,
      "s3",
      "https://uc.example",
      tokenProvider(),
      TableId,
      TableCatalog,
      TableSchema,
      TableName,
      TableLocation,
      awsCredentials(operation = CredentialOperation.READ)).asScala.toMap

    assert(props("fs.s3a.impl.original") === "com.example.CustomS3A")
    assert(props("fs.s3.impl.original") === "org.apache.hadoop.fs.s3a.S3AFileSystem")
    assert(props("fs.s3a.impl") === classOf[CredScopedFileSystem].getName)
    assert(props("fs.AbstractFileSystem.s3a.impl") === classOf[CredScopedFs].getName)
  }

  test("returns no credential properties for non-UC-vended schemes") {
    val props = CredPropsUtil.createTableCredProps(
      true,
      false,
      Map.empty[String, String].asJava,
      "file",
      "https://uc.example",
      tokenProvider(),
      TableId,
      TableCatalog,
      TableSchema,
      TableName,
      TableLocation,
      awsCredentials(operation = CredentialOperation.READ)).asScala.toMap

    assert(props.isEmpty)
  }

  private def tokenProvider(): TokenProvider = {
    TokenProvider.create(Map("type" -> "static", "token" -> "token").asJava)
  }

  private def awsCredentials(
      operation: CredentialOperation = CredentialOperation.READ_WRITE,
      expirationTimeMs: java.lang.Long = null): StorageCredential = {
    newCredential(
      operation = operation,
      s3AccessKeyId = "ak",
      s3SecretAccessKey = "sk",
      s3SessionToken = "st",
      expirationTimeMs = expirationTimeMs)
  }

  private def azureCredentials(
      operation: CredentialOperation = CredentialOperation.READ_WRITE,
      expirationTimeMs: java.lang.Long = null): StorageCredential = {
    newCredential(
      operation = operation,
      azureSasToken = "sas",
      expirationTimeMs = expirationTimeMs)
  }

  private def gcsCredentials(
      operation: CredentialOperation = CredentialOperation.READ_WRITE,
      expirationTimeMs: java.lang.Long = null): StorageCredential = {
    newCredential(
      operation = operation,
      gcsOauthToken = "token",
      expirationTimeMs = expirationTimeMs)
  }

  private def newCredential(
      operation: CredentialOperation,
      s3AccessKeyId: String = null,
      s3SecretAccessKey: String = null,
      s3SessionToken: String = null,
      azureSasToken: String = null,
      gcsOauthToken: String = null,
      expirationTimeMs: java.lang.Long = null): StorageCredential = {
    new StorageCredential(
      TableLocation,
      operation,
      new StorageCredentialConfig(
        s3AccessKeyId,
        s3SecretAccessKey,
        s3SessionToken,
        azureSasToken,
        gcsOauthToken),
      expirationTimeMs)
  }

  private def withHttpServer[T](body: HttpServer => T): T = {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    try {
      server.start()
      body(server)
    } finally {
      server.stop(0)
    }
  }

  private def sendJson(exchange: HttpExchange, status: Int, json: String): Unit = {
    val bytes = json.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json")
    exchange.sendResponseHeaders(status, bytes.length)
    val body = exchange.getResponseBody
    try {
      body.write(bytes)
    } finally {
      body.close()
    }
  }

  private def s3CredentialsResponseJson: String =
    """{
      |  "storage-credentials": [
      |    {
      |      "prefix": "s3://bucket/table",
      |      "operation": "READ_WRITE",
      |      "expiration-time-ms": 9999999999999,
      |      "config": {
      |        "s3.access-key-id": "renewed-ak",
      |        "s3.secret-access-key": "renewed-sk",
      |        "s3.session-token": "renewed-st"
      |      }
      |    }
      |  ]
      |}""".stripMargin
}
