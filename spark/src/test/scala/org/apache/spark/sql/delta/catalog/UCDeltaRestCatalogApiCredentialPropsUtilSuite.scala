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

package org.apache.spark.sql.delta.catalog

import scala.collection.JavaConverters._

import io.unitycatalog.client.auth.TokenProvider
import io.unitycatalog.client.model.{
  AwsCredentials,
  AzureUserDelegationSAS,
  GcpOauthToken,
  PathOperation,
  TableOperation,
  TemporaryCredentials
}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.delta.catalog.credentials.{
  CredPropsUtil,
  UCDeltaRestCatalogApiCredentialConf}
import org.apache.spark.sql.delta.catalog.credentials.fs.{CredScopedFileSystem, CredScopedFs}
import org.apache.spark.sql.delta.catalog.credentials.storage.{
  AbfsVendedTokenProvider,
  AwsVendedTokenProvider,
  GcsVendedTokenProvider
}

class UCDeltaRestCatalogApiCredentialPropsUtilSuite extends AnyFunSuite {

  test("creates static S3 credential properties") {
    val props = CredPropsUtil.createTableCredProps(
      false,
      false,
      Map.empty[String, String].asJava,
      "s3a",
      "https://uc.example",
      tokenProvider(),
      "table-id",
      TableOperation.READ_WRITE,
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
      "table-id",
      TableOperation.READ_WRITE,
      azureCredentials()).asScala.toMap
    val gcsProps = CredPropsUtil.createTableCredProps(
      false,
      false,
      Map.empty[String, String].asJava,
      "gs",
      "https://uc.example",
      tokenProvider(),
      "table-id",
      TableOperation.READ_WRITE,
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
      "table-id",
      TableOperation.READ_WRITE,
      awsCredentials(123L)).asScala.toMap

    assert(props(UCDeltaRestCatalogApiCredentialConf.S3A_CREDENTIALS_PROVIDER) ===
      classOf[AwsVendedTokenProvider].getName)
    assert(props(UCDeltaRestCatalogApiCredentialConf.UC_URI_KEY) === "https://uc.example")
    assert(props(UCDeltaRestCatalogApiCredentialConf.UC_AUTH_PREFIX + "type") === "static")
    assert(props(UCDeltaRestCatalogApiCredentialConf.UC_AUTH_PREFIX + "token") === "token")
    assert(props(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_KEY) ===
      UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_TABLE_VALUE)
    assert(props(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_ID_KEY) === "table-id")
    assert(props(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_OPERATION_KEY) === "READ_WRITE")
    assert(props(UCDeltaRestCatalogApiCredentialConf.S3A_INIT_ACCESS_KEY) === "ak")
    assert(props(UCDeltaRestCatalogApiCredentialConf.S3A_INIT_SECRET_KEY) === "sk")
    assert(props(UCDeltaRestCatalogApiCredentialConf.S3A_INIT_SESSION_TOKEN) === "st")
    assert(props(UCDeltaRestCatalogApiCredentialConf.S3A_INIT_CRED_EXPIRED_TIME) === "123")
    assert(props.contains(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_UID_KEY))
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
      PathOperation.PATH_CREATE_TABLE,
      azureCredentials(123L)).asScala.toMap
    val gcsProps = CredPropsUtil.createPathCredProps(
      true,
      false,
      Map.empty[String, String].asJava,
      "gs",
      "https://uc.example",
      tokenProvider(),
      "gs://bucket/table",
      PathOperation.PATH_CREATE_TABLE,
      gcsCredentials(456L)).asScala.toMap

    assert(azureProps(UCDeltaRestCatalogApiCredentialConf.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE) ===
      classOf[AbfsVendedTokenProvider].getName)
    assert(azureProps(UCDeltaRestCatalogApiCredentialConf.AZURE_INIT_SAS_TOKEN) === "sas")
    assert(
      azureProps(UCDeltaRestCatalogApiCredentialConf.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME) === "123")
    assert(
      azureProps(UCDeltaRestCatalogApiCredentialConf.UC_PATH_OPERATION_KEY) ===
        "PATH_CREATE_TABLE")

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
      "table-id",
      TableOperation.READ,
      awsCredentials()).asScala.toMap

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
      "table-id",
      TableOperation.READ,
      awsCredentials()).asScala.toMap

    assert(props.isEmpty)
  }

  private def tokenProvider(): TokenProvider = {
    TokenProvider.create(Map("type" -> "static", "token" -> "token").asJava)
  }

  private def awsCredentials(expirationTimeMs: java.lang.Long = null): TemporaryCredentials = {
    val aws = new AwsCredentials()
      .accessKeyId("ak")
      .secretAccessKey("sk")
      .sessionToken("st")
    new TemporaryCredentials()
      .awsTempCredentials(aws)
      .expirationTime(expirationTimeMs)
  }

  private def azureCredentials(expirationTimeMs: java.lang.Long = null): TemporaryCredentials = {
    val azure = new AzureUserDelegationSAS().sasToken("sas")
    new TemporaryCredentials()
      .azureUserDelegationSas(azure)
      .expirationTime(expirationTimeMs)
  }

  private def gcsCredentials(expirationTimeMs: java.lang.Long = null): TemporaryCredentials = {
    val gcs = new GcpOauthToken().oauthToken("token")
    new TemporaryCredentials()
      .gcpOauthToken(gcs)
      .expirationTime(expirationTimeMs)
  }
}
