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

import io.delta.kernel.*;
import io.delta.kernel.engine.Engine;

import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.snapshot.SnapshotManager;

public class TableImpl implements Table {
    public static Table forPath(Engine engine, String path) {
        String resolvedPath;
        try {
            resolvedPath = engine.getFileSystemClient().resolvePath(path);
        } catch (IOException io) {
            throw new RuntimeException(io);
        }
        return new TableImpl(resolvedPath);
    }

    private final SnapshotManager snapshotManager;
    private final String tablePath;

    public TableImpl(String tablePath) {
        this.tablePath = tablePath;
        final Path dataPath = new Path(tablePath);
        final Path logPath = new Path(dataPath, "_delta_log");
        this.snapshotManager = new SnapshotManager(logPath, dataPath);
    }

    @Override
    public String getPath(Engine engine) {
        return tablePath;
    }

    @Override
    public Snapshot getLatestSnapshot(Engine engine) throws TableNotFoundException {
        return snapshotManager.buildLatestSnapshot(engine);
    }

    @Override
    public Snapshot getSnapshotAsOfVersion(Engine engine, long versionId)
        throws TableNotFoundException {
        return snapshotManager.getSnapshotAt(engine, versionId);
    }

    @Override
    public Snapshot getSnapshotAsOfTimestamp(Engine engine, long millisSinceEpochUTC)
        throws TableNotFoundException {
        return snapshotManager.getSnapshotForTimestamp(engine, millisSinceEpochUTC);
    }

    @Override
    public void checkpoint(Engine engine, long version)
            throws TableNotFoundException, CheckpointAlreadyExistsException, IOException {
        snapshotManager.checkpoint(engine, version);
    }
}
