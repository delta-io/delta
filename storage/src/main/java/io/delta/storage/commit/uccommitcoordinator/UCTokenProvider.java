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

package io.delta.storage.commit.uccommitcoordinator;

import java.io.Closeable;

/**
 * Provider interface for Unity Catalog access tokens.
 *
 * <p>Implementations of this interface are responsible for providing valid access tokens
 * for authenticating with Unity Catalog services. The interface extends {@link Closeable} to allow
 * implementations to clean up resources such as HTTP clients or cached credentials.
 *
 * <p>Implementations may cache tokens and handle automatic renewal as needed.
 */
public interface UCTokenProvider extends Closeable {

  /**
   * Returns a valid access token for Unity Catalog authentication.
   *
   * @return a valid access token string
   */
  String accessToken();
}
