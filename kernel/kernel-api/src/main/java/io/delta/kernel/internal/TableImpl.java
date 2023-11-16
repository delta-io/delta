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

import java.io.FileNotFoundException;
import java.io.IOException;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.TableNotFoundException;
import io.delta.kernel.client.TableClient;

import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.snapshot.SnapshotManager;

public class TableImpl implements Table {
    public static Table forPath(TableClient tableClient, String path)
        throws TableNotFoundException {
        // Resolve the path to fully qualified table path using the `TableClient` APIs
        String resolvedPath;
        try {
            resolvedPath = tableClient.getFileSystemClient().resolvePath(path);
        } catch (FileNotFoundException fnf) {
            throw new TableNotFoundException(path, fnf);
        } catch (IOException io) {
            throw new RuntimeException(io);
        }
        final Path dataPath = new Path(resolvedPath);
        final Path logPath = new Path(dataPath, "_delta_log");

        return new TableImpl(logPath, dataPath);
    }

    private final Path logPath;
    private final Path dataPath;

    public TableImpl(Path logPath, Path dataPath) {
        this.logPath = logPath;
        this.dataPath = dataPath;
    }

    @Override
    public Snapshot getLatestSnapshot(TableClient tableClient) throws TableNotFoundException {
        return new SnapshotManager().buildLatestSnapshot(tableClient, logPath, dataPath);
    }
}
