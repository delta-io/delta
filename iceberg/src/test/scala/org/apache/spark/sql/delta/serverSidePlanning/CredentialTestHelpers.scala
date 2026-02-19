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

import org.apache.hadoop.conf.Configuration

/**
 * Test helpers for validating cloud provider credentials in server-side planning.
 * Each test case validates both credential parsing and Hadoop configuration injection.
 */
private[serverSidePlanning] object CredentialTestHelpers {

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
        "org.apache.spark.sql.delta.serverSidePlanning.ConfBasedGcsAccessTokenProvider"
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
