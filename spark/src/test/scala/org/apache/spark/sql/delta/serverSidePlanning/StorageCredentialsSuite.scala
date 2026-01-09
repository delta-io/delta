/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for StorageCredentials sealed trait and its subclasses.
 *
 * Tests verify:
 * - All credential types can be constructed with their required fields
 * - Pattern matching works correctly for each credential type
 * - The sealed trait ensures exhaustive matching
 */
class StorageCredentialsSuite extends AnyFunSuite {

  test("S3Credentials construction and pattern matching") {
    val s3Creds = S3Credentials(
      accessKeyId = "test-access-key-id",
      secretAccessKey = "test-secret-access-key",
      sessionToken = "test-session-token"
    )

    assert(s3Creds.accessKeyId == "test-access-key-id")
    assert(s3Creds.secretAccessKey == "test-secret-access-key")
    assert(s3Creds.sessionToken == "test-session-token")

    // Verify pattern matching works
    val result = s3Creds match {
      case S3Credentials(keyId, secret, token) =>
        (keyId, secret, token)
      case _ => fail("Should match S3Credentials")
    }

    assert(result._1 == "test-access-key-id")
    assert(result._2 == "test-secret-access-key")
    assert(result._3 == "test-session-token")
  }

  test("AzureCredentials construction and pattern matching") {
    val azureCreds = AzureCredentials(
      accountName = "mystorageaccount",
      sasToken = "sp=r&st=2023-01-01T00:00:00Z&se=2023-12-31T23:59:59Z&sv=2021-06-08&sr=c&sig=example",
      containerName = "mycontainer"
    )

    assert(azureCreds.accountName == "mystorageaccount")
    assert(azureCreds.sasToken == "sp=r&st=2023-01-01T00:00:00Z&se=2023-12-31T23:59:59Z&sv=2021-06-08&sr=c&sig=example")
    assert(azureCreds.containerName == "mycontainer")

    // Verify pattern matching works
    val result = azureCreds match {
      case AzureCredentials(account, sas, container) =>
        (account, sas, container)
      case _ => fail("Should match AzureCredentials")
    }

    assert(result._1 == "mystorageaccount")
    assert(result._2 == "sp=r&st=2023-01-01T00:00:00Z&se=2023-12-31T23:59:59Z&sv=2021-06-08&sr=c&sig=example")
    assert(result._3 == "mycontainer")
  }

  test("GcsCredentials construction and pattern matching") {
    val gcsCreds = GcsCredentials(
      oauth2Token = "test-gcs-oauth2-token"
    )

    assert(gcsCreds.oauth2Token == "test-gcs-oauth2-token")

    // Verify pattern matching works
    val result = gcsCreds match {
      case GcsCredentials(token) => token
      case _ => fail("Should match GcsCredentials")
    }

    assert(result == "test-gcs-oauth2-token")
  }

  test("StorageCredentials sealed trait allows exhaustive pattern matching") {
    val credentials: Seq[StorageCredentials] = Seq(
      S3Credentials("test-key", "test-secret", "test-token"),
      AzureCredentials("test-account", "test-sas", "test-container"),
      GcsCredentials("test-gcs-token")
    )

    // Verify we can pattern match exhaustively (compiler will warn if not exhaustive)
    credentials.foreach { cred =>
      val credType = cred match {
        case _: S3Credentials => "s3"
        case _: AzureCredentials => "azure"
        case _: GcsCredentials => "gcs"
      }
      assert(credType != null)
    }
  }

  test("StorageCredentials subclasses are case classes") {
    // Verify case class properties: equals, hashCode, toString, copy
    val s3a = S3Credentials("test-key-1", "test-secret-1", "test-token-1")
    val s3b = S3Credentials("test-key-1", "test-secret-1", "test-token-1")
    val s3c = S3Credentials("test-key-2", "test-secret-2", "test-token-2")

    // Structural equality
    assert(s3a == s3b)
    assert(s3a != s3c)

    // hashCode consistency
    assert(s3a.hashCode() == s3b.hashCode())

    // toString produces readable output
    assert(s3a.toString.contains("S3Credentials"))
    assert(s3a.toString.contains("test-key-1"))

    // copy method works
    val s3Modified = s3a.copy(accessKeyId = "test-new-key")
    assert(s3Modified.accessKeyId == "test-new-key")
    assert(s3Modified.secretAccessKey == "test-secret-1")
    assert(s3Modified.sessionToken == "test-token-1")
  }

  test("different credential types are not equal") {
    val s3 = S3Credentials("test-key", "test-secret", "test-token")
    val azure = AzureCredentials("test-account", "test-sas", "test-container")
    val gcs = GcsCredentials("test-gcs-token")

    assert(s3 != azure)
    assert(s3 != gcs)
    assert(azure != gcs)
  }
}
