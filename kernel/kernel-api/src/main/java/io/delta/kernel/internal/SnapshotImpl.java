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
import java.util.Optional;

import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.CommitCoordinatorClientHandler;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;

import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.CreateCheckpointIterator;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;
import static io.delta.kernel.internal.TableConfig.*;

/**
 * Implementation of {@link Snapshot}.
 */
public class SnapshotImpl implements Snapshot {
    private final Path logPath;
    private final Path dataPath;
    private final long version;
    private final LogReplay logReplay;
    private final Protocol protocol;
    private final Metadata metadata;
    private final LogSegment logSegment;
    private Optional<Long> inCommitTimestampOpt;

    public SnapshotImpl(
            Path dataPath,
            LogSegment logSegment,
            LogReplay logReplay,
            Protocol protocol,
            Metadata metadata) {
        this.logPath = new Path(dataPath, "_delta_log");
        this.dataPath = dataPath;
        this.version = logSegment.version;
        this.logSegment = logSegment;
        this.logReplay = logReplay;
        this.protocol = protocol;
        this.metadata = metadata;
        this.inCommitTimestampOpt = Optional.empty();
    }

    @Override
    public long getVersion(Engine engine) {
        return version;
    }

    @Override
    public StructType getSchema(Engine engine) {
        return getMetadata().getSchema();
    }

    @Override
    public ScanBuilder getScanBuilder(Engine engine) {
        return new ScanBuilderImpl(
            dataPath,
            protocol,
            metadata,
            getSchema(engine),
            logReplay, engine
        );
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public Protocol getProtocol() {
        return protocol;
    }

    public CreateCheckpointIterator getCreateCheckpointIterator(
            Engine engine) {
        long minFileRetentionTimestampMillis =
                System.currentTimeMillis() - TOMBSTONE_RETENTION.fromMetadata(metadata);
        return new CreateCheckpointIterator(engine,
                logSegment,
                minFileRetentionTimestampMillis
        );
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
    public Optional<Long> getLatestTransactionVersion(Engine engine, String applicationId) {
        return logReplay.getLatestTransactionIdentifier(engine, applicationId);
    }

    public LogSegment getLogSegment() {
        return logSegment;
    }

    public Path getLogPath() {
        return logPath;
    }

    public Path getDataPath() {
        return dataPath;
    }

    /**
     * Returns the timestamp of the latest commit of this snapshot.
     * For an uninitialized snapshot, this returns -1.
     * <p>
     * When InCommitTimestampTableFeature is enabled, the timestamp
     * is retrieved from the CommitInfo of the latest commit which
     * can result in an IO operation.
     * <p>
     * For non-ICT tables, this is the same as the file modification time of the latest commit in
     * the snapshot.
     *
     * @param engine the engine to use for IO operations
     * @return the timestamp of the latest commit
     */
    public long getTimestamp(Engine engine) {
        if (TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(metadata)) {
            if (!inCommitTimestampOpt.isPresent()) {
                try {
                    Optional<CommitInfo> commitInfoOpt = CommitInfo.getCommitInfoOpt(
                            engine, logPath, logSegment.version);
                    inCommitTimestampOpt = Optional.of(CommitInfo.getRequiredInCommitTimestamp(
                            commitInfoOpt,
                            String.valueOf(logSegment.version),
                            dataPath));
                } catch (IOException e) {
                    throw new RuntimeException("Failed to get inCommitTimestamp with IO", e);
                }
            }
            return inCommitTimestampOpt.get();
        } else {
            return logSegment.lastCommitTimestamp;
        }
    }

    public Optional<CommitCoordinatorClientHandler> getCommitCoordinatorClientHandlerOpt(
            Engine engine) {
        return COORDINATED_COMMITS_COORDINATOR_NAME.fromMetadata(metadata).map(
                commitCoordinatorStr -> engine.getCommitCoordinatorClientHandler(
                        commitCoordinatorStr,
                        COORDINATED_COMMITS_COORDINATOR_CONF.fromMetadata(metadata)));
    }
}
