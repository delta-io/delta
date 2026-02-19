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
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import shadedForDelta.org.apache.iceberg.{PartitionSpec, Table}
import shadedForDelta.org.apache.iceberg.catalog._

/**
 * Test suite for server-side planning credential handling.
 * Tests credential parsing and Hadoop configuration injection for S3, Azure, and GCS.
 */
class ServerSidePlanningCredentialsSuite extends QueryTest with SharedSparkSession {

  import CredentialTestHelpers._

  private val defaultNamespace = Namespace.of("testDatabase")
  private val defaultSchema = TestSchemas.testSchema
  private val defaultSpec = PartitionSpec.unpartitioned()

  private lazy val server = IcebergRESTServerTestUtils.startServer()
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

  test("Credentials: server response parsing and Hadoop configuration") {
    withTempTable("credentialsTest") { table =>
      populateTestData(s"rest_catalog.${defaultNamespace}.credentialsTest")

      val client = new IcebergRESTCatalogPlanningClient(serverUri, "test_catalog", "")
      try {
        // Covers the successful credential extraction and Hadoop configuration injection cases.
        val testCases: Seq[CredentialTestCase] = Seq(
          // S3
          S3CredentialTestCase(
            description = "S3 with session token",
            accessKeyId = "test-access-key",
            secretAccessKey = "test-secret-key",
            sessionToken = "test-session-token"
          ),

          // Azure without expiration
          AzureCredentialTestCase(
            description = "Azure without expiration",
            accountName = "unitycatalogmetastore",
            sasToken = "sv=2023-01-03&ss=b&srt=sco&sp=rwdlac&se=2025-12-31T23:59:59Z&sig=test",
            expirationMs = None
          ),

          // Azure with expiration
          AzureCredentialTestCase(
            description = "Azure with expiration",
            accountName = "unitycatalogmetastore",
            sasToken = "sv=2023-01-03&ss=b&srt=sco&sp=rwdlac&se=2025-12-31T23:59:59Z&sig=test",
            expirationMs = Some(1771456336352L)
          ),

          // GCS without expiration
          GcsCredentialTestCase(
            description = "GCS without expiration",
            token = "ya29.c.c0AY_VpZg_test_token",
            expirationMs = None
          ),

          // GCS with expiration
          GcsCredentialTestCase(
            description = "GCS with expiration",
            token = "ya29.c.c0AY_VpZg_test_token",
            expirationMs = Some(1771456336352L)
          )
        )

        testCases.foreach { testCase =>
          // Set server to return credentials.
          server.setTestCredentials(testCase.serverResponse.asJava)

          val scanPlan = client.planScan(defaultNamespace.toString, "credentialsTest")

          assert(scanPlan.credentials.isDefined,
            s"[${testCase.description}] Credentials should be present in ScanPlan")

          val testConf = new Configuration()
          scanPlan.credentials.foreach(_.configure(testConf))

          // Validate Hadoop config matches expectation.
          testCase.expectedHadoopConfig.foreach { case (key, expectedValue) =>
            val actualValue = testConf.get(key)
            assert(actualValue == expectedValue,
              s"[${testCase.description}] Hadoop config mismatch for key '$key'.\n" +
              s"Expected: $expectedValue\n" +
              s"Got: $actualValue")
          }

          // Clear for next test case
          server.clearCaptured()
        }
      } finally {
        client.close()
      }
    }
  }

  test("incomplete/missing credentials throw errors") {
    withTempTable("incompleteCredsTest") { table =>
      populateTestData(s"rest_catalog.${defaultNamespace}.incompleteCredsTest")

      val client = new IcebergRESTCatalogPlanningClient(serverUri, "test_catalog", "")
      try {
        // Test cases for incomplete credentials that should throw errors
        val errorTestCases = Seq(
          ("Incomplete S3 (missing secret and token)",
            Map("s3.access-key-id" -> "test-key"),
            "Missing required credential"),
          ("GCS incomplete: only expiration",
            Map("gcs.oauth2.token-expires-at" -> "1771456336352"),
            "Unrecognized credential keys"),
          // Expiration-only Azure entry is unrecognized: without the token key
          // (adls.sas-token.<account>), hasAzureKeys() returns false and we can't
          // construct valid credentials.
          ("Azure incomplete: expiration key only, no token key",
            Map("adls.sas-token-expires-at-ms.myaccount.dfs.core.windows.net" -> "1771456336352"),
            "Unrecognized credential keys")
        )

        errorTestCases.foreach { case (description, incompleteConfig, expectedMessageFragment) =>
          // Configure server with incomplete credentials
          server.setTestCredentials(incompleteConfig.asJava)

          // Verify that planScan throws IllegalStateException
          val exception = intercept[IllegalStateException] {
            client.planScan(defaultNamespace.toString, "incompleteCredsTest")
          }

          // Verify error message contains relevant fragment
          assert(exception.getMessage.contains(expectedMessageFragment),
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

  /**
   * Convenience wrapper for withTempTable that uses the test suite's default values.
   */
  private def withTempTable[T](tableName: String)(func: Table => T): T = {
    IcebergRESTServerTestUtils.withTempTable(
      catalog, defaultNamespace, tableName,
      defaultSchema, defaultSpec, Some(server)
    )(func)
  }

  /**
   * Convenience wrapper for populateTestData that uses the test suite's SparkSession.
   */
  private def populateTestData(tableName: String): Unit = {
    IcebergRESTServerTestUtils.populateTestData(spark, tableName)
  }

  /**
   * Credential test helper traits and case classes.
   * Private to this test suite - these are test-only utilities.
   */
  private object CredentialTestHelpers {

    /**
     * Test case for end-to-end credential validation.
     *
     * Flow: serverResponse → (client parses) → creds.configure(conf) → expectedHadoopConfig
     */
    sealed trait CredentialTestCase {
      /** Test case name */
      def description: String

      /** Cloud provider type (S3, Azure, GCS) */
      def cloudProvider: String

      /** INPUT: Credential config map that server returns */
      def serverResponse: Map[String, String]

      /** EXPECTED OUTPUT: Hadoop configuration keys and values */
      def expectedHadoopConfig: Map[String, String]
    }

    /**
     * S3 credential test case.
     */
    case class S3CredentialTestCase(
        description: String,
        accessKeyId: String,
        secretAccessKey: String,
        sessionToken: String
    ) extends CredentialTestCase {
      override def cloudProvider: String = "S3"

      override def serverResponse: Map[String, String] = Map(
        "s3.access-key-id" -> accessKeyId,
        "s3.secret-access-key" -> secretAccessKey,
        "s3.session-token" -> sessionToken
      )

      override def expectedHadoopConfig: Map[String, String] = Map(
        "fs.s3a.path.style.access" -> "true",
        "fs.s3.impl.disable.cache" -> "true",
        "fs.s3a.impl.disable.cache" -> "true",
        "fs.s3a.access.key" -> accessKeyId,
        "fs.s3a.secret.key" -> secretAccessKey,
        "fs.s3a.session.token" -> sessionToken
      )
    }

    /**
     * Azure credential test case.
     */
    case class AzureCredentialTestCase(
        description: String,
        accountName: String,
        sasToken: String,
        expirationMs: Option[Long] = None
    ) extends CredentialTestCase {
      override def cloudProvider: String = "Azure"

      override def serverResponse: Map[String, String] = {
        val base = Map(
          s"adls.sas-token.$accountName.dfs.core.windows.net" -> sasToken
        )
        expirationMs match {
          case Some(ms) =>
            val expiryKey = s"adls.sas-token-expires-at-ms.$accountName.dfs.core.windows.net"
            base + (expiryKey -> ms.toString)
          case None => base
        }
      }

      override def expectedHadoopConfig: Map[String, String] = {
        val accountSuffix = s"$accountName.dfs.core.windows.net"
        Map(
          "fs.abfs.impl.disable.cache" -> "true",
          "fs.abfss.impl.disable.cache" -> "true",
          s"fs.azure.account.auth.type.$accountSuffix" -> "SAS",
          s"fs.azure.sas.fixed.token.$accountSuffix" -> sasToken
        )
      }
    }

    /**
     * GCS credential test case.
     */
    case class GcsCredentialTestCase(
        description: String,
        token: String,
        expirationMs: Option[Long] = None
    ) extends CredentialTestCase {
      override def cloudProvider: String = "GCS"

      override def serverResponse: Map[String, String] = {
        val base = Map("gcs.oauth2.token" -> token)
        expirationMs match {
          case Some(ms) => base + ("gcs.oauth2.token-expires-at" -> ms.toString)
          case None => base
        }
      }

      override def expectedHadoopConfig: Map[String, String] = {
        val base = Map(
          "fs.gs.impl.disable.cache" -> "true",
          "fs.gs.auth.type" -> "ACCESS_TOKEN_PROVIDER",
          "fs.gs.auth.access.token.provider.impl" ->
            "org.apache.spark.sql.delta.serverSidePlanning.FixedGcsAccessTokenProvider",
          "fs.gs.auth.access.token" -> token
        )
        expirationMs match {
          case Some(ms) => base + ("fs.gs.auth.access.token.expiration.ms" -> ms.toString)
          case None => base
        }
      }
    }
  }

}
