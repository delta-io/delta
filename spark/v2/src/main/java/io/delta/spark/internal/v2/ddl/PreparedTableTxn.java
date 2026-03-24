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
 * Base DTO for a prepared (but not yet committed) Kernel transaction.
 *
 * <p>Holds the Kernel {@link Transaction} and the {@link Engine} that was used to build it, so that
 * the committer can call {@link Transaction#commit} without needing to reconstruct the engine.
 */
public class PreparedTableTxn {
  private final Transaction transaction;
  private final Engine engine;

  public PreparedTableTxn(Transaction transaction, Engine engine) {
    this.transaction = requireNonNull(transaction, "transaction is null");
    this.engine = requireNonNull(engine, "engine is null");
  }

  public Transaction getTransaction() {
    return transaction;
  }

  public Engine getEngine() {
    return engine;
  }
}
