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

import java.io.IOException;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.TableNotFoundException;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.client.TableClient;

import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.snapshot.SnapshotManager;

public class TableImpl implements Table {
    public static Table forPath(TableClient tableClient, String path) {
        // Resolve the path to fully qualified table path using the `TableClient` APIs
        String resolvedPath;
        try {
            resolvedPath = tableClient.getFileSystemClient().resolvePath(path);
        } catch (IOException io) {
            throw new RuntimeException(io);
        }
        return new TableImpl(resolvedPath);
    }

    private final SnapshotManager snapshotManager;
    private final Path logPath;
    private final Path tablePath;

    public TableImpl(String tablePath) {
        this.tablePath = new Path(tablePath);
        this.logPath = new Path(tablePath, "_delta_log");
        this.snapshotManager = new SnapshotManager(logPath, this.tablePath);
    }

    @Override
    public Snapshot getLatestSnapshot(TableClient tableClient) throws TableNotFoundException {
        return snapshotManager.buildLatestSnapshot(tableClient);
    }

    @Override
    public String getPath() {
        return tablePath.toString();
    }

    @Override
    public Snapshot getSnapshotAsOfVersion(TableClient tableClient, long versionId)
        throws TableNotFoundException {
        return snapshotManager.getSnapshotAt(tableClient, versionId);
    }

    @Override
    public Snapshot getSnapshotAsOfTimestamp(TableClient tableClient, long millisSinceEpochUTC)
        throws TableNotFoundException {
        return snapshotManager.getSnapshotForTimestamp(tableClient, millisSinceEpochUTC);
    }

    @Override
    public TransactionBuilder createTransactionBuilder(
            TableClient tableClient,
            String engineInfo,
            String operation) {
        return new TransactionBuilderImpl(this, engineInfo, operation);
    }

    @Override
    public void checkpoint(TableClient tableClient, long version) {
        throw new UnsupportedOperationException("NYI");
    }
}
