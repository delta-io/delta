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

/** Exception thrown when no Delta table exists at the requested path. */
public class TableNotFoundException extends RuntimeException {

  private final String tablePath;

  public TableNotFoundException(String tablePath) {
    super(String.format("Delta table does not exist at path: %s.", tablePath));
    this.tablePath = tablePath;
  }

  public String getTablePath() {
    return tablePath;
  }
}
