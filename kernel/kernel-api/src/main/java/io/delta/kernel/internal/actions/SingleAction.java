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
public class SingleAction
{
    public static SingleAction fromRow(Row row, TableClient tableClient)
    {
        if (!row.isNullAt(0)) {
            final SetTransaction txn = SetTransaction.fromRow(row.getStruct(0));
            return new SingleAction(txn, null, null, null, null, null, null);
        }

        if (!row.isNullAt(1)) {
            final AddFile add = AddFile.fromRow(row.getStruct(1));
            return new SingleAction(null, add, null, null, null, null, null);
        }

        if (!row.isNullAt(2)) {
            final RemoveFile remove = RemoveFile.fromRow(row.getStruct(2));
            return new SingleAction(null, null, remove, null, null, null, null);
        }

        if (!row.isNullAt(3)) {
            final Metadata metadata = Metadata.fromRow(row.getStruct(3), tableClient);
            return new SingleAction(null, null, null, metadata, null, null, null);
        }

        if (!row.isNullAt(4)) {
            final Protocol protocol = Protocol.fromRow(row.getStruct(4));
            return new SingleAction(null, null, null, null, protocol, null, null);
        }

        if (!row.isNullAt(5)) {
            final AddCDCFile cdc = AddCDCFile.fromRow(row.getStruct(5));
            return new SingleAction(null, null, null, null, null, cdc, null);
        }

        if (!row.isNullAt(6)) {
            final CommitInfo commitInfo = CommitInfo.fromRow(row.getStruct(6));
            return new SingleAction(null, null, null, null, null, null, commitInfo);
        }

        throw new IllegalStateException("SingleAction row contained no non-null actions");
    }

    public static StructType READ_SCHEMA = new StructType()
        .add("txn", SetTransaction.READ_SCHEMA)
        .add("add", AddFile.READ_SCHEMA)
        .add("remove", RemoveFile.READ_SCHEMA)
        .add("metaData", Metadata.READ_SCHEMA)
        .add("protocol", Protocol.READ_SCHEMA)
        .add("cdc", AddCDCFile.READ_SCHEMA)
        .add("commitInfo", CommitInfo.READ_SCHEMA);

    private final SetTransaction txn;
    private final AddFile add;
    private final RemoveFile remove;
    private final Metadata metadata;
    private final Protocol protocol;
    private final AddCDCFile cdc;
    private final CommitInfo commitInfo;

    private SingleAction(
        SetTransaction txn,
        AddFile add,
        RemoveFile remove,
        Metadata metadata,
        Protocol protocol,
        AddCDCFile cdc,
        CommitInfo commitInfo)
    {
        this.txn = txn;
        this.add = add;
        this.remove = remove;
        this.metadata = metadata;
        this.protocol = protocol;
        this.cdc = cdc;
        this.commitInfo = commitInfo;
    }

    public Action unwrap()
    {
        if (txn != null) {
            return txn;
        }
        if (add != null) {
            return add;
        }
        if (remove != null) {
            return remove;
        }
        if (metadata != null) {
            return metadata;
        }
        if (protocol != null) {
            return protocol;
        }
        if (cdc != null) {
            return cdc;
        }
        if (commitInfo != null) {
            return commitInfo;
        }

        throw new IllegalStateException("SingleAction row contained no non-null actions");
    }
}
