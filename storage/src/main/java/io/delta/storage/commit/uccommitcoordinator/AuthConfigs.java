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

package io.delta.storage.commit.uccommitcoordinator;

/** Internal class - not intended for direct use. */
class AuthConfigs {
  private AuthConfigs() {}

  // Define the authentication type, where the type can be:
  // 1. static: which uses the StaticTokenProvider to generate the static token.
  // 2. oauth: which uses the OAuthTokenProvider to generate the oauth token.
  // 3. fully qualified class name of the custom TokenProvider implementation.
  static final String TYPE = "type";

  // Configure keys for static token provider.
  static final String STATIC_TYPE_VALUE = "static";
  static final String STATIC_TOKEN = "token";

  // Configure keys for oauth token provider.
  static final String OAUTH_TYPE_VALUE = "oauth";
  static final String OAUTH_URI = "oauth.uri";
  static final String OAUTH_CLIENT_ID = "oauth.clientId";
  static final String OAUTH_CLIENT_SECRET = "oauth.clientSecret";
}
