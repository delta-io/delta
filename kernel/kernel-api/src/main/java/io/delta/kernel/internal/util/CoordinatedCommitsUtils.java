/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.util;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.CommitCoordinatorClientHandler;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.JsonHandler;
import io.delta.kernel.engine.coordinatedcommits.actions.AbstractCommitInfo;
import io.delta.kernel.engine.coordinatedcommits.actions.AbstractMetadata;
import io.delta.kernel.engine.coordinatedcommits.actions.AbstractProtocol;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.fs.Path;
import static io.delta.kernel.internal.TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF;
import static io.delta.kernel.internal.TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME;
import static io.delta.kernel.internal.util.FileNames.getUnbackfilledDeltaFile;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

public class CoordinatedCommitsUtils {

    public static Optional<CommitCoordinatorClientHandler> getCommitCoordinatorClientHandler(
            Engine engine, Metadata metadata, Protocol protocol) {
        return COORDINATED_COMMITS_COORDINATOR_NAME.fromMetadata(engine, metadata)
                .map(commitCoordinatorStr -> {
                    checkArgument(protocol.isFeatureSupported("coordinatedCommits-preview"));
                    return engine.getCommitCoordinatorClientHandler(
                            commitCoordinatorStr,
                            COORDINATED_COMMITS_COORDINATOR_CONF.fromMetadata(engine, metadata));
                });
    }

    public static Tuple2<Optional<String>, Map<String, String>> getCoordinatedCommitsConfs(
            Engine engine, Metadata metadata) {
        Optional<String> coordinatorName = COORDINATED_COMMITS_COORDINATOR_NAME
                .fromMetadata(engine, metadata);
        Map<String, String> coordinatorConf;
        if (coordinatorName.isPresent()) {
            coordinatorConf = COORDINATED_COMMITS_COORDINATOR_CONF.fromMetadata(engine, metadata);
        } else {
            coordinatorConf = new HashMap<>();
        }
        return new Tuple2<>(coordinatorName, coordinatorConf);
    }

    public static AbstractMetadata convertMetadataToAbstractMetadata(Metadata metadata) {
        return new AbstractMetadata() {
            @Override
            public String getId() {
                return metadata.getId();
            }

            @Override
            public String getName() {
                return metadata.getName().orElse(null);
            }

            @Override
            public String getDescription() {
                return metadata.getDescription().orElse(null);
            }

            @Override
            public String getProvider() {
                return metadata.getFormat().getProvider();
            }

            @Override
            public Map<String, String> getFormatOptions() {
                // Assuming Format class has a method to get format options
                return metadata.getFormat().getOptions();
            }

            @Override
            public String getSchemaString() {
                // Assuming Metadata class has a method to get schema string
                return metadata.getSchemaString();
            }

            @Override
            public List<String> getPartitionColumns() {
                // Assuming Metadata class has a method to get partition columns
                return VectorUtils.toJavaList(metadata.getPartitionColumns());
            }

            @Override
            public Map<String, String> getConfiguration() {
                return metadata.getConfiguration();
            }

            @Override
            public Optional<Long> getCreatedTime() {
                return metadata.getCreatedTime();
            }
        };
    }

    public static AbstractProtocol convertProtocolToAbstractProtocol(Protocol protocol) {
        return new AbstractProtocol() {
            @Override
            public int getMinReaderVersion() {
                return protocol.getMinReaderVersion();
            }

            @Override
            public int getMinWriterVersion() {
                return protocol.getMinWriterVersion();
            }

            @Override
            public Set<String> getReaderFeatures() {
                return new HashSet<>(protocol.getReaderFeatures());
            }

            @Override
            public Set<String> getWriterFeatures() {
                return new HashSet<>(protocol.getWriterFeatures());
            }
        };
    }

    public static AbstractCommitInfo convertCommitInfoToAbstractCommitInfo(CommitInfo commitInfo) {
        return () -> commitInfo.getInCommitTimestamp().orElse(commitInfo.getTimestamp());
    }

    /**
     * This method takes care of backfilling any unbackfilled delta files when coordinated commits
     * is not enabled on the table (i.e., commit-coordinator is not present) but there are still
     * unbackfilled delta files in the table. This can happen if an error occurred during the
     * CC to FS commit where the commit-coordinator was able to register the downgrade commit but
     * it failed to backfill it. This method must be invoked before doing the next commit as
     * otherwise there will be a gap in the backfilled commit sequence.
     */
    public static void backfillWhenCoordinatedCommitsDisabled(
            Engine engine, SnapshotImpl snapshot, Logger logger) throws IOException {
        if (snapshot.getTableCommitCoordinatorClientHandlerOpt(engine).isPresent()) {
            // Coordinated commits is enabled on the table.
            // Don't backfill as backfills are managed by commit-coordinators.
            return;
        }

        List<FileNames.PathVersionUuid> unbackfilledPathVersionUuid = snapshot
                .getLogSegment().deltas.stream()
                .map(deltaFile -> getUnbackfilledDeltaFile(new Path(deltaFile.getPath())))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        if (unbackfilledPathVersionUuid.isEmpty()) return;

        JsonHandler handler = engine.getJsonHandler();

        for (FileNames.PathVersionUuid pathVersionUuid : unbackfilledPathVersionUuid) {
            Path unbackfilledDeltaFile = pathVersionUuid.getPath();
            long version = pathVersionUuid.getVersion();
            String backfilledFilePath = FileNames.deltaFile(snapshot.getLogPath(), version);
            try {
                CloseableIterator<Row> rows = getRowsFromFile(handler, unbackfilledDeltaFile);
                handler.writeJsonFileAtomically(backfilledFilePath, rows, false);
                logger.info(
                        "Delta file {} backfilled to path {}.",
                        unbackfilledDeltaFile,
                        backfilledFilePath);
            } catch (FileAlreadyExistsException ignored) {
                logger.info("Delta file {} already backfilled.", unbackfilledDeltaFile);
            }
        }
    }

    private static CloseableIterator<Row> getRowsFromFile(
            JsonHandler handler, Path delta) throws IOException {
        FileStatus file = FileStatus.of(delta.toString(), 0, 0);
        CloseableIterator<ColumnarBatch> columnarBatches = handler.readJsonFiles(
                Utils.singletonCloseableIterator(file),
                SingleAction.FULL_SCHEMA,
                Optional.empty());

        List<Row> allRows = new ArrayList<>();

        while (columnarBatches.hasNext()) {
            ColumnarBatch batch = columnarBatches.next();
            CloseableIterator<Row> rows = batch.getRows();
            while (rows.hasNext()) {
                Row row = rows.next();
                allRows.add(row);
            }
        }

        return Utils.toCloseableIterator(allRows.iterator());
    }
}
