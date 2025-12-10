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

package org.apache.spark.sql.delta.coordinatedcommits

import java.net.{URI, URISyntaxException}

import io.delta.storage.commit.uccommitcoordinator.{OAuthUCTokenProvider, UCClient}

import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.internal.MDC

/**
 * Abstract sealed trait for Unity Catalog client parameters.
 * Implementations provide different authentication mechanisms (token-based or OAuth).
 */
sealed trait UCClientParams {

  /** The Unity Catalog server URI */
  def uri: String

  /**
   * Builds a UCClient using the appropriate authentication mechanism.
   *
   * @param ucClientFactory Factory for creating UCClient instances
   * @return A configured UCClient
   */
  def buildUCClient(ucClientFactory: UCClientFactory): UCClient
}

/**
 * Unity Catalog client parameters for token-based authentication.
 *
 * @param uri   The Unity Catalog server URI
 * @param token The authentication token
 */
case class UCTokenClientParams(uri: String, token: String) extends UCClientParams {
  override def buildUCClient(ucClientFactory: UCClientFactory): UCClient = {
    ucClientFactory.createUCClient(uri, token)
  }
}

/**
 * Unity Catalog client parameters for OAuth-based authentication.
 *
 * @param uri               The Unity Catalog server URI
 * @param oauthUri          The OAuth token endpoint URI
 * @param oauthClientId     The OAuth client ID
 * @param oauthClientSecret The OAuth client secret
 */
case class UCOAuthClientParams(
    uri: String,
    oauthUri: String,
    oauthClientId: String,
    oauthClientSecret: String) extends UCClientParams {
  override def buildUCClient(ucClientFactory: UCClientFactory): UCClient = {
    val provider = new OAuthUCTokenProvider(oauthUri, oauthClientId, oauthClientSecret)
    ucClientFactory.createUCClient(uri, provider)
  }
}

object UCClientParams extends DeltaLogging {

  /**
   * Factory method to create UCClientParams from optional configuration values.
   * Returns None if the configuration is invalid or incomplete.
   *
   * @param catalogName       The catalog name.
   * @param uri               The Unity Catalog server URI
   * @param token             The authentication token (for token-based auth)
   * @param oauthUri          The OAuth token endpoint URI (for OAuth auth)
   * @param oauthClientId     The OAuth client ID (for OAuth auth)
   * @param oauthClientSecret The OAuth client secret (for OAuth auth)
   * @return Some(UCClientParams) if valid, None otherwise
   */
  def create(
      catalogName: String,
      uri: Option[String],
      token: Option[String] = None,
      oauthUri: Option[String] = None,
      oauthClientId: Option[String] = None,
      oauthClientSecret: Option[String] = None): Option[UCClientParams] = {
    // Validate the uri.
    uri match {
      case Some(u) =>
        if (!isValidURI(u)) {
          logWarning(log"Skipping catalog ${MDC(DeltaLogKeys.CATALOG, catalogName)} as it " +
            log"does not have a valid URI ${MDC(DeltaLogKeys.URI, u)}.")
          return None
        }
      case None => return None
    }

    (uri, token, oauthUri, oauthClientId, oauthClientSecret) match {
      case (Some(u), Some(t), _, _, _) =>
        // Use fixed token to build the UCTokenClientParams.
        Some(UCTokenClientParams(uri = u, token = t))
      case (Some(u), _, Some(oUri), Some(oClientId), Some(oClientSecret)) =>
        // Validate the OAuth URI.
        if (!isValidURI(oUri)) {
          logWarning(log"Skipping catalog ${MDC(DeltaLogKeys.CATALOG, catalogName)} " +
            log"as it does not have a valid OAuth URI")
          return None
        }
        // Use OAuth credentials to build the UCOAuthClientParams.
        Some(UCOAuthClientParams(
          uri = u,
          oauthUri = oUri,
          oauthClientId = oClientId,
          oauthClientSecret = oClientSecret))
      case _ =>
        logWarning(log"Skipping catalog ${MDC(DeltaLogKeys.CATALOG, catalogName)} as it does " +
          "not have configured fixed token or oauth credential in Spark Session.")
        None
    }
  }

  private def isValidURI(uri: String): Boolean = {
    try {
      new URI(uri)
      true
    } catch {
      case _: URISyntaxException => false
    }
  }
}
