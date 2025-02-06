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

package io.delta.kernel.ccv2;

import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.ccv2.internal.ResolvedTableImpl;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;

public interface ResolvedTable {

  /** Create a {@link ResolvedTable} object using the given {@link ResolvedMetadata}. */
  static ResolvedTable forResolvedMetadata(Engine engine, ResolvedMetadata resolvedMetadata) {
    return new ResolvedTableImpl(engine, resolvedMetadata);
  }

  /**
   * @return the {@link Snapshot} of the table at the current version.
   * @throws TableNotFoundException if the table is not found
   */
  Snapshot getSnapshot();

  /**
   * Create a {@link TransactionBuilder} which can create a {@link Transaction} object to mutate the
   * table.
   */
  TransactionBuilder createTransactionBuilder(String engineInfo, Operation operation);
}
