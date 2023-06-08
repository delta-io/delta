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
package io.delta.kernel.internal.actions;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;

/**
 * Helper method to decode a Log file Action to its specific type.
 */
public class SingleAction {
    public static SingleAction fromRow(Row row, TableClient tableClient) {
        final SetTransaction txn = SetTransaction.fromRow(row.getRecord(0));
        if (txn != null) {
            return new SingleAction(txn, null, null, null, null, null);
        }

        final AddFile add = AddFile.fromRow(row.getRecord(1));
        if (add != null) {
            return new SingleAction(null, add, null, null, null, null);
        }

        final RemoveFile remove = RemoveFile.fromRow(row.getRecord(2));
        if (remove != null) {
            return new SingleAction(null, null, remove, null, null, null);
        }

        final Metadata metadata = Metadata.fromRow(row.getRecord(3), tableClient);
        if (metadata != null) {
            return new SingleAction(null, null, null, metadata, null, null);
        }

        final Protocol protocol = Protocol.fromRow(row.getRecord(4));
        if (protocol != null) {
            return new SingleAction(null, null, null, null, protocol, null);
        }

        final CommitInfo commitInfo = CommitInfo.fromRow(row.getRecord(5));
        if (commitInfo != null) {
            return new SingleAction(null, null, null, null, null, commitInfo);
        }

        throw new IllegalStateException("SingleAction row contained no non-null actions");
    }

    public static StructType READ_SCHEMA = new StructType()
        .add("txn", SetTransaction.READ_SCHEMA)
        .add("add", AddFile.READ_SCHEMA)
        .add("remove", RemoveFile.READ_SCHEMA)
        .add("metaData", Metadata.READ_SCHEMA)
        .add("protocol", Protocol.READ_SCHEMA)
        .add("commitInfo", CommitInfo.READ_SCHEMA);

    private final SetTransaction txn;
    private final AddFile add;
    private final RemoveFile remove;
    private final Metadata metadata;
    private final Protocol protocol;
    private final CommitInfo commitInfo;

    private SingleAction(
            SetTransaction txn,
            AddFile add,
            RemoveFile remove,
            Metadata metadata,
            Protocol protocol,
            CommitInfo commitInfo) {
        this.txn = txn;
        this.add = add;
        this.remove = remove;
        this.metadata = metadata;
        this.protocol = protocol;
        this.commitInfo = commitInfo;
    }

    public Action unwrap() {
        if (txn != null) return txn;
        if (add != null) return add;
        if (remove != null) return remove;
        if (metadata != null) return metadata;
        if (protocol != null) return protocol;
        if (commitInfo != null) return commitInfo;

        throw new IllegalStateException("SingleAction row contained no non-null actions");
    }
}
