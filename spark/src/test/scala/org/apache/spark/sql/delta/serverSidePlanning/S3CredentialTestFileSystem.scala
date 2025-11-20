/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import org.apache.hadoop.fs.Path

/**
 * Companion object for S3CredentialTestFileSystem.
 * Allows tests to set expected credential values for validation.
 */
object S3CredentialTestFileSystem {
  @volatile private var expectedAccessKey: Option[String] = None
  @volatile private var expectedSecretKey: Option[String] = None
  @volatile private var expectedSessionToken: Option[String] = None

  /**
   * Set expected credentials that will be validated during file access.
   * Call this before executing queries that should use these credentials.
   */
  def setExpectedCredentials(accessKey: String, secretKey: String, sessionToken: String): Unit = {
    expectedAccessKey = Some(accessKey)
    expectedSecretKey = Some(secretKey)
    expectedSessionToken = Some(sessionToken)
  }

  /**
   * Clear expected credentials after test completes.
   */
  def clearExpectedCredentials(): Unit = {
    expectedAccessKey = None
    expectedSecretKey = None
    expectedSessionToken = None
  }

  /**
   * Get expected credentials for validation.
   */
  private[serverSidePlanning] def getExpectedCredentials: (
      Option[String], Option[String], Option[String]) = {
    (expectedAccessKey, expectedSecretKey, expectedSessionToken)
  }
}

/**
 * Test filesystem for validating S3 credentials.
 * Verifies that temporary AWS credentials from server-side planning
 * were correctly injected into Hadoop configuration.
 */
class S3CredentialTestFileSystem extends CredentialTestFileSystem {

  override def scheme(): String = "s3a"

  override def checkCredentials(path: Path): Unit = {
    val conf = getConf

    // Get actual credentials from Hadoop config
    val accessKey = conf.get("fs.s3a.access.key")
    val secretKey = conf.get("fs.s3a.secret.key")
    val sessionToken = conf.get("fs.s3a.session.token")

    // Verify required S3 credentials are present
    assert(accessKey != null && accessKey.nonEmpty,
      "fs.s3a.access.key must be set")
    assert(secretKey != null && secretKey.nonEmpty,
      "fs.s3a.secret.key must be set")
    assert(sessionToken != null && sessionToken.nonEmpty,
      "fs.s3a.session.token must be set")

    // If expected credentials are set, verify exact values match
    val (expectedAccess, expectedSecret, expectedSession) =
      S3CredentialTestFileSystem.getExpectedCredentials
    expectedAccess.foreach { expected =>
      assert(accessKey == expected,
        s"fs.s3a.access.key mismatch: expected '$expected', got '$accessKey'")
    }
    expectedSecret.foreach { expected =>
      assert(secretKey == expected,
        s"fs.s3a.secret.key mismatch: expected '$expected', got '$secretKey'")
    }
    expectedSession.foreach { expected =>
      assert(sessionToken == expected,
        s"fs.s3a.session.token mismatch: expected '$expected', got '$sessionToken'")
    }
  }
}
