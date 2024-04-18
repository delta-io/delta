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
package io.delta.kernel.internal;

import java.util.List;

import io.delta.kernel.*;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;

import io.delta.kernel.internal.lang.CloseableIterable;

public class TransactionImpl implements Transaction {

    @Override
    public Row getState(TableClient tableClient) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public List<String> getPartitionColumns(TableClient tableClient) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public StructType getSchema(TableClient tableClient) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public TransactionCommitStatus commitWithRetries(
            TableClient tableClient,
            CloseableIterable<Row> stagedData,
            int maxRetries)
            throws ConcurrentWriteException {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
