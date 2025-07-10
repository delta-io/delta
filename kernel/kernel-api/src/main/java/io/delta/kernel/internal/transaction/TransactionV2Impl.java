/*
 * Copyright (2023) The Delta Lake Project Authors.
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

package io.delta.kernel.internal.transaction;

import io.delta.kernel.commit.CommitContext;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.transaction.TransactionV2;
import io.delta.kernel.utils.CloseableIterator;

public class TransactionV2Impl implements TransactionV2 {
  @Override
  public Row getTransactionState() {
    return null;
  }

  @Override
  public CommitContext getInitialCommitContext(Engine engine, CloseableIterator<Row> dataActions) {
    return null;
  }
}
