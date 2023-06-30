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

import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.Tuple2;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;

/**
 * Implementation of {@link Snapshot}.
 */
public class SnapshotImpl implements Snapshot
{
    private final Path dataPath;
    private final long version;

    private final LogReplay logReplay;
    private final Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata;

    public SnapshotImpl(
        Path logPath,
        Path dataPath,
        long version,
        LogSegment logSegment,
        TableClient tableClient,
        long timestamp)
    {
        this.dataPath = dataPath;
        this.version = version;

        this.logReplay = new LogReplay(
            logPath,
            dataPath,
            tableClient,
            logSegment);
        this.protocolAndMetadata = logReplay.lazyLoadProtocolAndMetadata();
    }

    @Override
    public long getVersion(TableClient tableClient)
    {
        return version;
    }

    @Override
    public StructType getSchema(TableClient tableClient)
    {
        return getMetadata().getSchema();
    }

    @Override
    public ScanBuilder getScanBuilder(TableClient tableClient)
    {
        return new ScanBuilderImpl(
            dataPath,
            protocolAndMetadata,
            getSchema(tableClient),
            logReplay.getAddFiles(),
            tableClient
        );
    }

    public Metadata getMetadata()
    {
        return protocolAndMetadata.get()._2;
    }

    public Protocol getProtocol()
    {
        return protocolAndMetadata.get()._1;
    }
}
