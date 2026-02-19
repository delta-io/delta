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

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpGet
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicHeader
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import shadedForDelta.org.apache.iceberg.{PartitionSpec, Table}
import shadedForDelta.org.apache.iceberg.catalog._
import shadedForDelta.org.apache.iceberg.rest.IcebergRESTServer

/**
 * Test suite for server-side planning credential handling.
 * Tests credential parsing and Hadoop configuration injection for S3, Azure, and GCS.
 */
class ServerSidePlanningCredentialsSuite extends QueryTest with SharedSparkSession {

  import testImplicits._
  import CredentialTestHelpers._

  private val defaultNamespace = Namespace.of("testDatabase")
  private val defaultSchema = TestSchemas.testSchema
  private val defaultSpec = PartitionSpec.unpartitioned()

  private lazy val server = startServer()
  private lazy val catalog = server.getCatalog()
  private lazy val serverUri = s"http://localhost:${server.getPort}"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Configure Spark to use the Iceberg REST catalog
    spark.conf.set(s"spark.sql.catalog.rest_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(s"spark.sql.catalog.rest_catalog.type", "rest")
    spark.conf.set(s"spark.sql.catalog.rest_catalog.uri", serverUri)

    if (catalog.isInstanceOf[SupportsNamespaces]) {
      catalog.asInstanceOf[SupportsNamespaces].createNamespace(defaultNamespace)
    } else {
      throw new IllegalStateException("Catalog does not support namespaces")
    }
  }

  override def afterAll(): Unit = {
    try {
      if (server != null) {
        server.clearCaptured()
        server.stop()
      }
    } finally {
      super.afterAll()
    }
  }

  test("ScanPlan with cloud provider credentials") {
    withTempTable("credentialsTest") { table =>
      populateTestData(s"rest_catalog.${defaultNamespace}.credentialsTest")

      val client = new IcebergRESTCatalogPlanningClient(serverUri, "test_catalog", "")
      try {
        // Test cases for all three cloud providers with and without expiration
        val testCases: Seq[CredentialTestCase] = Seq(
          // S3 test case
          S3CredentialTestCase(
            description = "S3 with session token",
            credentialConfig = Map(
              "s3.access-key-id" -> "test-access-key",
              "s3.secret-access-key" -> "test-secret-key",
              "s3.session-token" -> "test-session-token"),
            expectedAccessKeyId = "test-access-key",
            expectedSecretAccessKey = "test-secret-key",
            expectedSessionToken = "test-session-token"),

          // Azure WITHOUT expiration
          AzureCredentialTestCase(
            description = "Azure without expiration",
            credentialConfig = Map(
              "adls.sas-token.unitycatalogmetastore.dfs.core.windows.net" ->
                "sv=2023-01-03&ss=b&srt=sco&sp=rwdlac&se=2025-12-31T23:59:59Z&sig=test"),
            expectedAccountName = "unitycatalogmetastore",
            expectedSasToken = "sv=2023-01-03&ss=b&srt=sco&sp=rwdlac&se=2025-12-31T23:59:59Z&sig=test",
            hasExpiration = false),

          // Azure WITH expiration
          AzureCredentialTestCase(
            description = "Azure with expiration",
            credentialConfig = Map(
              "adls.sas-token.unitycatalogmetastore.dfs.core.windows.net" ->
                "sv=2023-01-03&ss=b&srt=sco&sp=rwdlac&se=2025-12-31T23:59:59Z&sig=test",
              "adls.sas-token-expires-at-ms.unitycatalogmetastore.dfs.core.windows.net" ->
                "1771456336352"),
            expectedAccountName = "unitycatalogmetastore",
            expectedSasToken = "sv=2023-01-03&ss=b&srt=sco&sp=rwdlac&se=2025-12-31T23:59:59Z&sig=test",
            hasExpiration = true),

          // GCS WITHOUT expiration
          GcsCredentialTestCase(
            description = "GCS without expiration",
            credentialConfig = Map(
              "gcs.oauth2.token" -> "ya29.c.c0AY_VpZg_test_token"),
            expectedToken = "ya29.c.c0AY_VpZg_test_token",
            expectedExpiration = None), // The server didn't provide an expiration, so this is None. The 1-hour default happens later when the
                                        // token provider is instantiated and asked for a token, not during credential parsing.

          // GCS WITH expiration
          GcsCredentialTestCase(
            description = "GCS with expiration",
            credentialConfig = Map(
              "gcs.oauth2.token" -> "ya29.c.c0AY_VpZg_test_token",
              "gcs.oauth2.token-expires-at" -> "1771456336352"),
            expectedToken = "ya29.c.c0AY_VpZg_test_token",
            expectedExpiration = Some(1771456336352L))
        )

        testCases.foreach { testCase =>
          // Configure server to return test credentials
          server.setTestCredentials(testCase.credentialConfig.asJava)

          // Call planScan
          val scanPlan = client.planScan(defaultNamespace.toString, "credentialsTest")

          // Validate credential parsing
          assert(scanPlan.credentials.isDefined,
            s"[${testCase.description}] Credentials should be present in ScanPlan")
          testCase.validateCredentials(scanPlan.credentials.get)

          // Validate Hadoop config injection by simulating what the factory does
          val testConf = new Configuration()
          scanPlan.credentials.foreach { creds =>
            creds match {
              case S3Credentials(accessKeyId, secretAccessKey, sessionToken) =>
                testConf.set("fs.s3a.path.style.access", "true")
                testConf.set("fs.s3.impl.disable.cache", "true")
                testConf.set("fs.s3a.impl.disable.cache", "true")
                testConf.set("fs.s3a.access.key", accessKeyId)
                testConf.set("fs.s3a.secret.key", secretAccessKey)
                testConf.set("fs.s3a.session.token", sessionToken)

              case AzureCredentials(accountName, credentialEntries) =>
                val accountSuffix = s"$accountName.dfs.core.windows.net"
                val sasTokenKey = credentialEntries.keys
                  .find(key => key.startsWith("adls.sas-token") && !key.contains("expires-at-ms"))
                  .get
                val sasTokenValue = credentialEntries(sasTokenKey)
                testConf.set("fs.abfs.impl.disable.cache", "true")
                testConf.set("fs.abfss.impl.disable.cache", "true")
                testConf.set(s"fs.azure.account.auth.type.$accountSuffix", "SAS")
                testConf.set(s"fs.azure.sas.fixed.token.$accountSuffix", sasTokenValue)

              case GcsCredentials(oauth2Token, expirationEpochMs) =>
                testConf.set("fs.gs.impl.disable.cache", "true")
                testConf.set("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER")
                val gcsProviderClass =
                  "org.apache.spark.sql.delta.serverSidePlanning.FixedGcsAccessTokenProvider"
                testConf.set("fs.gs.auth.access.token.provider", gcsProviderClass)
                testConf.set("fs.gs.auth.access.token", oauth2Token)
                expirationEpochMs.foreach(ms =>
                  testConf.set("fs.gs.auth.access.token.expiration.ms", ms.toString))
            }
          }
          testCase.validateHadoopConfig(testConf)

          // Clear for next test case
          server.clearCaptured()
        }
      } finally {
        client.close()
      }
    }
  }

  test("ScanPlan with no credentials") {
    withTempTable("noCredentialsTest") { table =>
      populateTestData(s"rest_catalog.${defaultNamespace}.noCredentialsTest")

      val client = new IcebergRESTCatalogPlanningClient(serverUri, "test_catalog", "")
      try {
        // Don't configure any credentials
        val scanPlan = client.planScan(defaultNamespace.toString, "noCredentialsTest")

        // Verify credentials are absent
        assert(scanPlan.credentials.isEmpty,
          "Credentials should be None when server doesn't return any")
      } finally {
        client.close()
      }
    }
  }

  test("incomplete credentials throw errors") {
    withTempTable("incompleteCredsTest") { table =>
      populateTestData(s"rest_catalog.${defaultNamespace}.incompleteCredsTest")

      val client = new IcebergRESTCatalogPlanningClient(serverUri, "test_catalog", "")
      try {
        // Test cases for incomplete credentials that should throw errors
        val errorTestCases = Seq(
          ("Incomplete S3 (missing secret and token)",
            Map("s3.access-key-id" -> "test-key"),
            "s3.secret-access-key"),
          ("GCS incomplete: only expiration", 
            Map("gcs.oauth2.token-expires-at" -> "1771456336352"),
            "gcs.oauth2.token")
          // Note: Azure with Unity Catalog format doesn't have the same notion of "incomplete"
          // credentials as S3/GCS. Either we have the full adls.sas-token.* key or we don't.
        )

        errorTestCases.foreach { case (description, incompleteConfig, expectedMessageFragment) =>
          // Configure server with incomplete credentials
          server.setTestCredentials(incompleteConfig.asJava)

          // Verify that planScan throws IllegalStateException
          val exception = intercept[IllegalStateException] {
            client.planScan(defaultNamespace.toString, "incompleteCredsTest")
          }

          // Verify error message contains relevant fragment
          assert(exception.getMessage.toLowerCase.contains(expectedMessageFragment.toLowerCase),
            s"[$description] Error message should contain '$expectedMessageFragment'. " +
            s"Got: ${exception.getMessage}")

          // Clear for next test case
          server.clearCaptured()
        }
      } finally {
        client.close()
      }
    }
  }

  private def startServer(): IcebergRESTServer = {
    val config = Map(IcebergRESTServer.REST_PORT -> "0").asJava
    val newServer = new IcebergRESTServer(config)
    newServer.start(/* join = */ false)
    if (!isServerReachable(newServer)) {
      throw new IllegalStateException("Failed to start IcebergRESTServer")
    }
    newServer
  }

  private def isServerReachable(server: IcebergRESTServer): Boolean = {
    val httpHeaders = Map(
      HttpHeaders.ACCEPT -> ContentType.APPLICATION_JSON.getMimeType,
      HttpHeaders.CONTENT_TYPE -> ContentType.APPLICATION_JSON.getMimeType
    ).map { case (k, v) => new BasicHeader(k, v) }.toSeq.asJava

    val httpClient = HttpClientBuilder.create()
      .setDefaultHeaders(httpHeaders)
      .build()

    try {
      val httpGet = new HttpGet(s"http://localhost:${server.getPort}/v1/config")
      val httpResponse = httpClient.execute(httpGet)
      try {
        val statusCode = httpResponse.getStatusLine.getStatusCode
        statusCode == 200
      } finally {
        httpResponse.close()
      }
    } finally {
      httpClient.close()
    }
  }

  private def withTempTable[T](tableName: String)(func: Table => T): T = {
    val tableId = TableIdentifier.of(defaultNamespace, tableName)
    val table = catalog.createTable(tableId, defaultSchema, defaultSpec)
    try {
      func(table)
    } finally {
      catalog.dropTable(tableId, false)
      server.clearCaptured()
    }
  }

  /**
   * Populates a table with sample test data covering all schema types.
   * Uses parallelize with 2 partitions to create 2 data files.
   */
  private def populateTestData(tableName: String): Unit = {
    import org.apache.spark.sql.Row

    val data = spark.sparkContext.parallelize(0 until 250, numSlices = 2)
      .map(i => Row(
        i, // intCol
        i.toLong, // longCol
        i * 10.0, // doubleCol
        i.toFloat, // floatCol
        s"test_$i", // stringCol
        i % 2 == 0, // boolCol
        BigDecimal(i).bigDecimal, // decimalCol
        java.sql.Date.valueOf("2024-01-01"), // dateCol
        java.sql.Timestamp.valueOf("2024-01-01 00:00:00"), // timestampCol
        java.sql.Date.valueOf("2024-01-01"), // localDateCol
        java.sql.Timestamp.valueOf("2024-01-01 00:00:00"), // localDateTimeCol
        java.sql.Timestamp.valueOf("2024-01-01 00:00:00"), // instantCol
        Row(i * 100), // address.intCol (nested struct)
        Row(s"meta_$i"), // metadata.stringCol (nested struct)
        Row(s"child_$i"), // parent.`child.name` (nested struct with dotted field name)
        s"city_$i", // address.city (literal top-level dotted column)
        s"abc_$i" // a.b.c (literal top-level dotted column)
      ))

    spark.createDataFrame(data, TestSchemas.sparkSchema)
      .write
      .format("iceberg")
      .mode("append")
      .save(tableName)
  }

  /**
   * Credential test helper traits and case classes.
   * Private to this test suite - these are test-only utilities.
   */
  private object CredentialTestHelpers {

    /**
     * Base trait for credential test cases.
     * Implementations validate both:
     * 1. Credential parsing from IRC server response
     * 2. Hadoop configuration injection for file system access
     */
    sealed trait CredentialTestCase {
      def description: String
      def cloudProvider: String
      def credentialConfig: Map[String, String]
      def validateCredentials(credentials: ScanPlanStorageCredentials): Unit
      def validateHadoopConfig(conf: Configuration): Unit
    }

    /**
     * S3 credential test case.
     */
    case class S3CredentialTestCase(
        description: String,
        credentialConfig: Map[String, String],
        expectedAccessKeyId: String,
        expectedSecretAccessKey: String,
        expectedSessionToken: String
    ) extends CredentialTestCase {
      override def cloudProvider: String = "S3"

      override def validateCredentials(credentials: ScanPlanStorageCredentials): Unit = {
        assert(credentials.isInstanceOf[S3Credentials],
          s"[$description] Expected S3Credentials but got ${credentials.getClass.getSimpleName}")
        val s3Creds = credentials.asInstanceOf[S3Credentials]
        assert(s3Creds.accessKeyId == expectedAccessKeyId,
          s"[$description] Expected accessKeyId=$expectedAccessKeyId but got ${s3Creds.accessKeyId}")
        assert(s3Creds.secretAccessKey == expectedSecretAccessKey,
          s"[$description] Expected secretAccessKey=$expectedSecretAccessKey but got ${s3Creds.secretAccessKey}")
        assert(s3Creds.sessionToken == expectedSessionToken,
          s"[$description] Expected sessionToken=$expectedSessionToken but got ${s3Creds.sessionToken}")
      }

      override def validateHadoopConfig(conf: Configuration): Unit = {
        assert(conf.get("fs.s3a.access.key") == expectedAccessKeyId,
          s"[$description] S3 access key not configured correctly")
        assert(conf.get("fs.s3a.secret.key") == expectedSecretAccessKey,
          s"[$description] S3 secret key not configured correctly")
        assert(conf.get("fs.s3a.session.token") == expectedSessionToken,
          s"[$description] S3 session token not configured correctly")
        assert(conf.get("fs.s3a.path.style.access") == "true",
          s"[$description] S3 path style access not enabled")
        assert(conf.get("fs.s3.impl.disable.cache") == "true",
          s"[$description] S3 FileSystem cache not disabled")
        assert(conf.get("fs.s3a.impl.disable.cache") == "true",
          s"[$description] S3A FileSystem cache not disabled")
      }
    }

    /**
     * Azure credential test case.
     */
    case class AzureCredentialTestCase(
        description: String,
        credentialConfig: Map[String, String],
        expectedAccountName: String,
        expectedSasToken: String,
        hasExpiration: Boolean = false
    ) extends CredentialTestCase {
      override def cloudProvider: String = "Azure"

      override def validateCredentials(credentials: ScanPlanStorageCredentials): Unit = {
        assert(credentials.isInstanceOf[AzureCredentials],
          s"[$description] Expected AzureCredentials but got ${credentials.getClass.getSimpleName}")
        val azureCreds = credentials.asInstanceOf[AzureCredentials]
        assert(azureCreds.accountName == expectedAccountName,
          s"[$description] Expected accountName=$expectedAccountName but got ${azureCreds.accountName}")

        // Validate credential entries contain expected keys
        val tokenKey = s"adls.sas-token.$expectedAccountName.dfs.core.windows.net"
        assert(azureCreds.credentialEntries.contains(tokenKey),
          s"[$description] Credential entries should contain key: $tokenKey")
        assert(azureCreds.credentialEntries(tokenKey) == expectedSasToken,
          s"[$description] Expected SAS token=$expectedSasToken but got ${azureCreds.credentialEntries(tokenKey)}")

        if (hasExpiration) {
          val expiryKey = s"adls.sas-token-expires-at-ms.$expectedAccountName.dfs.core.windows.net"
          assert(azureCreds.credentialEntries.contains(expiryKey),
            s"[$description] Credential entries should contain expiry key: $expiryKey")
        }
      }

      override def validateHadoopConfig(conf: Configuration): Unit = {
        val accountSuffix = s"$expectedAccountName.dfs.core.windows.net"
        assert(conf.get(s"fs.azure.account.auth.type.$accountSuffix") == "SAS",
          s"[$description] Expected SAS auth type for $accountSuffix")
        assert(conf.get(s"fs.azure.sas.fixed.token.$accountSuffix") == expectedSasToken,
          s"[$description] Expected SAS token=$expectedSasToken for $accountSuffix")
        assert(conf.get("fs.abfs.impl.disable.cache") == "true",
          s"[$description] ABFS FileSystem cache not disabled")
        assert(conf.get("fs.abfss.impl.disable.cache") == "true",
          s"[$description] ABFSS FileSystem cache not disabled")
      }
    }

    /**
     * GCS credential test case.
     */
    case class GcsCredentialTestCase(
        description: String,
        credentialConfig: Map[String, String],
        expectedToken: String,
        expectedExpiration: Option[Long] = None
    ) extends CredentialTestCase {
      override def cloudProvider: String = "GCS"

      override def validateCredentials(credentials: ScanPlanStorageCredentials): Unit = {
        assert(credentials.isInstanceOf[GcsCredentials],
          s"[$description] Expected GcsCredentials but got ${credentials.getClass.getSimpleName}")
        val gcsCreds = credentials.asInstanceOf[GcsCredentials]
        assert(gcsCreds.oauth2Token == expectedToken,
          s"[$description] Expected token=$expectedToken but got ${gcsCreds.oauth2Token}")
        assert(gcsCreds.expirationEpochMs == expectedExpiration,
          s"[$description] Expected expiration=$expectedExpiration but got ${gcsCreds.expirationEpochMs}")
      }

      override def validateHadoopConfig(conf: Configuration): Unit = {
        assert(conf.get("fs.gs.auth.type") == "ACCESS_TOKEN_PROVIDER",
          s"[$description] Expected ACCESS_TOKEN_PROVIDER auth type")
        val expectedProviderClass =
          "org.apache.spark.sql.delta.serverSidePlanning.FixedGcsAccessTokenProvider"
        assert(conf.get("fs.gs.auth.access.token.provider") == expectedProviderClass,
          s"[$description] Expected provider class=$expectedProviderClass")
        assert(conf.get("fs.gs.auth.access.token") == expectedToken,
          s"[$description] Expected token=$expectedToken")
        assert(conf.get("fs.gs.impl.disable.cache") == "true",
          s"[$description] GCS FileSystem cache not disabled")

        expectedExpiration match {
          case Some(expMs) =>
            assert(conf.get("fs.gs.auth.access.token.expiration.ms") == expMs.toString,
              s"[$description] Expected expiration=$expMs")
          case None =>
            assert(conf.get("fs.gs.auth.access.token.expiration.ms") == null,
              s"[$description] Expected no expiration config")
        }
      }
    }
  }

}
