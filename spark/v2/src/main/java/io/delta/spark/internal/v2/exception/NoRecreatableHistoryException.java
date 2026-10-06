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
package io.delta.spark.internal.v2.exception;

/** Exception thrown when the Delta log has no recreatable history at the requested path. */
public class NoRecreatableHistoryException extends RuntimeException {

  private final String tablePath;

  public NoRecreatableHistoryException(String tablePath) {
    super(String.format("No recreatable commits found at path: %s.", tablePath));
    this.tablePath = tablePath;
  }

  public String getTablePath() {
    return tablePath;
  }
}
