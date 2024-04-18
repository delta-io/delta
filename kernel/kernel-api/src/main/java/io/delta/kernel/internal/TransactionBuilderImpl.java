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

import java.util.*;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import io.delta.kernel.*;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.*;

import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.snapshot.SnapshotHint;
import io.delta.kernel.internal.util.Tuple2;
import static io.delta.kernel.internal.util.VectorUtils.stringArrayValue;
import static io.delta.kernel.internal.util.VectorUtils.stringStringMapValue;

public class TransactionBuilderImpl implements TransactionBuilder {

    public TransactionBuilderImpl(TableImpl table, String engineInfo, String operation) {
    }

    @Override
    public TransactionBuilder withSchema(TableClient tableClient, StructType newSchema) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public TransactionBuilder withPartitionColumns(
            TableClient tableClient,
            Set<String> partitionColumns) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public TransactionBuilder withReadSet(
            TableClient tableClient,
            long readVersion,
            Predicate readPredicate) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public TransactionBuilder withTransactionId(
            TableClient tableClient,
            String applicationId,
            long transactionVersion) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Transaction build(TableClient tableClient) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
