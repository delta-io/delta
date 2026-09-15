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
package io.delta.kernel.internal.checkpoints;

import java.nio.charset.StandardCharsets;

/** Capture complete contents of a {@code _last_checkpoint} file. */
public final class LastCheckpointSerialized {
  private final String json;

  public LastCheckpointSerialized(String json) {
    this.json = json;
  }

  public String json() {
    return json;
  }

  public byte[] utf8Bytes() {
    return json.getBytes(StandardCharsets.UTF_8);
  }

  /** Returns the captured JSON blob unchanged. */
  public String toJson() {
    return json;
  }
}
