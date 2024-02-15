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

import java.util.Optional;

import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.types.StructType;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.snapshot.SnapshotHint;

/**
 * Implementation of {@link Snapshot}.
 */
public class SnapshotImpl implements Snapshot {
    private final Path dataPath;
    private final long version;
    private final LogReplay logReplay;
    private final Protocol protocol;
    private final Metadata metadata;

    public SnapshotImpl(
            Path logPath,
            Path dataPath,
            long version,
            LogSegment logSegment,
            TableClient tableClient,
            long timestamp,
            Optional<SnapshotHint> snapshotHint) {
        this.dataPath = dataPath;
        this.version = version;
        this.logReplay = new LogReplay(
            logPath,
            dataPath,
            version,
            tableClient,
            logSegment,
            snapshotHint);
        this.protocol = logReplay.getProtocol();
        this.metadata = logReplay.getMetadata();
    }

    @Override
    public long getVersion(TableClient tableClient) {
        return version;
    }

    @Override
    public StructType getSchema(TableClient tableClient) {
        return getMetadata().getSchema();
    }

    @Override
    public ScanBuilder getScanBuilder(TableClient tableClient) {
        return new ScanBuilderImpl(
            dataPath,
            protocol,
            metadata,
            getSchema(tableClient),
            logReplay,
            tableClient
        );
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public Protocol getProtocol() {
        return protocol;
    }

    /**
     * Get the latest transaction version for given <i>applicationId</i>. This information comes
     * from the transactions identifiers stored in Delta transaction log. This API is not a public
     * API. For now keep this internal to enable Flink upgrade to use Kernel.
     *
     * @param applicationId Identifier of the application that put transaction identifiers in
     *                      Delta transaction log
     * @return Last transaction version or {@link Optional#empty()} if no transaction identifier
     * exists for this application.
     */
    public Optional<Long> getLatestTransactionVersion(String applicationId) {
        return logReplay.getLatestTransactionIdentifier(applicationId);
    }
}
