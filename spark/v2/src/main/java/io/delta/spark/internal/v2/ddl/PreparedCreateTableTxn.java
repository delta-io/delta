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
package io.delta.spark.internal.v2.ddl;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Transaction;
import io.delta.kernel.engine.Engine;

/**
 * Prepared CREATE TABLE transaction DTO.
 *
 * <p>Extends {@link PreparedTableTxn} with the table path, so that the post-commit phase can
 * identify the table location for snapshot loading and catalog publication.
 */
public class PreparedCreateTableTxn extends PreparedTableTxn {
  private final String tablePath;

  public PreparedCreateTableTxn(Transaction transaction, Engine engine, String tablePath) {
    super(transaction, engine);
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
  }

  public String getTablePath() {
    return tablePath;
  }
}
