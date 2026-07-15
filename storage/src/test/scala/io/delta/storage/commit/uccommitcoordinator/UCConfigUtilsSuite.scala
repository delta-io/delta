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

package io.delta.storage.commit.uccommitcoordinator

import scala.collection.JavaConverters._

import org.scalatest.funsuite.AnyFunSuite

class UCConfigUtilsSuite extends AnyFunSuite {

  private def config(entries: (String, String)*): java.util.Map[String, String] =
    entries.toMap.asJava

  test("extractUri returns the uri when present") {
    assert(UCConfigUtils.extractUri(config("uri" -> "https://uc.example.com")) ===
      "https://uc.example.com")
  }

  test("extractUri throws when uri is missing") {
    val e = intercept[IllegalArgumentException] {
      UCConfigUtils.extractUri(config("auth.token" -> "t"))
    }
    assert(e.getMessage.contains("uri"))
  }

  test("extractCaseSensitiveAuthConfig prefers auth.* and preserves camelCase keys") {
    val auth = UCConfigUtils.extractCaseSensitiveAuthConfig(config(
      "uri" -> "https://uc.example.com",
      "auth.type" -> "oauth",
      "auth.oauth.clientId" -> "id",
      "auth.oauth.clientSecret" -> "secret",
      "token" -> "legacy-should-be-ignored"))
    assert(auth.get("type") === "oauth")
    assert(auth.get("oauth.clientId") === "id")
    assert(auth.get("oauth.clientSecret") === "secret")
    assert(!auth.containsKey("token"))
  }

  test("extractCaseSensitiveAuthConfig falls back to legacy token") {
    val auth = UCConfigUtils.extractCaseSensitiveAuthConfig(config(
      "uri" -> "https://uc.example.com",
      "token" -> "my-token"))
    assert(auth.get("type") === "static")
    assert(auth.get("token") === "my-token")
  }

  test("extractCaseSensitiveAuthConfig returns empty when no auth is present") {
    assert(UCConfigUtils.extractCaseSensitiveAuthConfig(
      config("uri" -> "https://uc.example.com")).isEmpty)
  }

  test("hasAuthConfig detects auth.* keys and legacy token") {
    assert(UCConfigUtils.hasAuthConfig(config("auth.type" -> "static", "auth.token" -> "t")))
    assert(UCConfigUtils.hasAuthConfig(config("token" -> "t")))
    assert(!UCConfigUtils.hasAuthConfig(config("uri" -> "https://uc.example.com")))
  }

  test("extractAppVersions strips the prefix and ignores other keys") {
    val versions = UCConfigUtils.extractAppVersions(config(
      "uri" -> "https://uc.example.com",
      "appVersions.Kernel" -> "0.7.0",
      "appVersions.Delta V2 connector" -> "true"))
    assert(versions.asScala === Map("Kernel" -> "0.7.0", "Delta V2 connector" -> "true"))
  }

  test("isDeltaRestApiEnabled defaults to true and honors explicit values") {
    assert(UCConfigUtils.isDeltaRestApiEnabled(config("uri" -> "u")))
    assert(UCConfigUtils.isDeltaRestApiEnabled(config("deltaRestApi.enabled" -> "true")))
    assert(!UCConfigUtils.isDeltaRestApiEnabled(config("deltaRestApi.enabled" -> "false")))
  }

  test("credential flags default to true and honor explicit values") {
    assert(UCConfigUtils.isCredentialRenewalEnabled(config("uri" -> "u")))
    assert(UCConfigUtils.isCredentialScopedFsEnabled(config("uri" -> "u")))
    assert(!UCConfigUtils.isCredentialRenewalEnabled(config("renewCredential.enabled" -> "false")))
    assert(!UCConfigUtils.isCredentialScopedFsEnabled(config("credScopedFs.enabled" -> "false")))
  }

  test("parseBoolean uses the default only when the key is absent") {
    assert(UCConfigUtils.parseBoolean(config("k" -> "true"), "k", false))
    assert(!UCConfigUtils.parseBoolean(config("k" -> "false"), "k", true))
    assert(UCConfigUtils.parseBoolean(config("k" -> "TRUE"), "k", false))
    assert(UCConfigUtils.parseBoolean(config("other" -> "x"), "k", true))
    assert(!UCConfigUtils.parseBoolean(config("other" -> "x"), "k", false))
  }

  test("parseBoolean throws on a present but invalid value") {
    val e = intercept[IllegalArgumentException] {
      UCConfigUtils.parseBoolean(config("k" -> "yes"), "k", false)
    }
    assert(e.getMessage.contains("k"))
  }

  test("prefix matching is case-sensitive") {
    assert(!UCConfigUtils.hasAuthConfig(config("Auth.type" -> "static")))
    assert(UCConfigUtils.extractCaseSensitiveAuthConfig(config("AUTH.token" -> "t")).isEmpty)
    assert(UCConfigUtils.extractAppVersions(config("AppVersions.Kernel" -> "0.7.0")).isEmpty)
  }
}
