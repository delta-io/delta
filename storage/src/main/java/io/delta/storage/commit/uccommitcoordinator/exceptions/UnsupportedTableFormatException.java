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

package io.delta.storage.commit.uccommitcoordinator.exceptions;

import java.io.IOException;

/**
 * Thrown when the catalog refuses to serve a non-Delta table. Callers should fall back to a
 * non-Delta-REST-API load path. Emitted as HTTP 400 with {@code error.type =
 * "UnsupportedTableFormatException"} on the wire.
 */
public class UnsupportedTableFormatException extends IOException {
  public UnsupportedTableFormatException(String message) {
    super(message);
  }

  public UnsupportedTableFormatException(String message, Throwable cause) {
    super(message, cause);
  }
}
