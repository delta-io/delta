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

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.TableNotFoundException;
import io.delta.kernel.client.TableClient;

import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.snapshot.SnapshotManager;
import io.delta.kernel.internal.util.Logging;

public class TableImpl implements Table, Logging
{
    public static Table forPath(String path)
    {
        final Path dataPath = new Path(path);
        final Path logPath = new Path(dataPath, "_delta_log");

        return new TableImpl(logPath, dataPath);
    }

    private final Path logPath;
    private final Path dataPath;

    public TableImpl(Path logPath, Path dataPath)
    {
        this.logPath = logPath;
        this.dataPath = dataPath;
    }

    @Override
    public Snapshot getLatestSnapshot(TableClient tableClient)
        throws TableNotFoundException
    {
        return new SnapshotManager().buildLatestSnapshot(tableClient, logPath, dataPath);
    }
}
