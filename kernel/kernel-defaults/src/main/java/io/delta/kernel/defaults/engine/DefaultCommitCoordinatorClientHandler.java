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
package io.delta.kernel.defaults.engine;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.delta.storage.LogStore;
import io.delta.storage.commit.CommitCoordinatorClient;
import io.delta.storage.commit.CommitFailedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import io.delta.kernel.commit.Commit;
import io.delta.kernel.commit.CommitResponse;
import io.delta.kernel.commit.GetCommitsResponse;
import io.delta.kernel.commit.UpdatedActions;
import io.delta.kernel.commit.actions.AbstractCommitInfo;
import io.delta.kernel.commit.actions.AbstractMetadata;
import io.delta.kernel.commit.actions.AbstractProtocol;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.CommitCoordinatorClientHandler;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.kernel.defaults.internal.coordinatedcommits.CommitCoordinatorProvider;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.defaults.internal.logstore.LogStoreProvider;

/**
 * Default implementation of {@link CommitCoordinatorClientHandler} based on Hadoop APIs.
 * It takes a Hadoop {@link Configuration} object to interact with the commit coordinator client.
 * The following optional configurations can be set to customize the behavior of the client:
 * <ul>
 *     <li>{@code io.delta.kernel.logStore.<scheme>.impl} - The class name of the custom
 *     {@link LogStore} implementation to use for operations on storage systems with the
 *     specified {@code scheme}. For example, to use a custom {@link LogStore} for S3 storage
 *     objects:
 *     <pre>{@code
 *     <property>
 *       <name>io.delta.kernel.logStore.s3.impl</name>
 *       <value>com.example.S3LogStore</value>
 *     </property>
 *     }</pre>
 *     If not set, the default LogStore implementation for the scheme will be used.
 *     </li>
 *     <li>{@code delta.enableFastS3AListFrom} - Set to {@code true} to enable fast listing
 *     functionality when using a {@link LogStore} created for S3 storage objects.
 *     </li>
 * </ul>
 */
public class DefaultCommitCoordinatorClientHandler implements CommitCoordinatorClientHandler {
    private final Configuration hadoopConf;
    private final CommitCoordinatorClient commitCoordinatorClient;

    /**
     * Create an instance of the default {@link DefaultCommitCoordinatorClientHandler}
     * implementation.
     *
     * @param hadoopConf Configuration to use. List of options to customize the behavior of
     *                   the client can be found in the class documentation.
     */
    public DefaultCommitCoordinatorClientHandler(
            Configuration hadoopConf, String name, Map<String, String> conf) {
        this.hadoopConf = hadoopConf;
        this.commitCoordinatorClient = CommitCoordinatorProvider
                .getCommitCoordinatorClient(name, conf);
    }

    @Override
    public Map<String, String> registerTable(
            String logPath,
            long currentVersion,
            AbstractMetadata currentMetadata,
            AbstractProtocol currentProtocol) {
        return commitCoordinatorClient.registerTable(
                new Path(logPath),
                currentVersion,
                convertAbstractMetadata(currentMetadata),
                convertAbstractProtocol(currentProtocol));
    }

    @Override
    public CommitResponse commit(
            String logPath,
            Map<String, String> tableConf,
            long commitVersion,
            CloseableIterator<Row> actions,
            UpdatedActions updatedActions)
            throws IOException, io.delta.kernel.commit.CommitFailedException {
        Path path = new Path(logPath);
        LogStore logStore = LogStoreProvider.getLogStore(hadoopConf, path.toUri().getScheme());
        try {
            return convertCommitResponse(commitCoordinatorClient.commit(
                    logStore,
                    hadoopConf,
                    path,
                    tableConf,
                    commitVersion,
                    new Iterator<String>() {
                        @Override
                        public boolean hasNext() {
                            return actions.hasNext();
                        }

                        @Override
                        public String next() {
                            return JsonUtils.rowToJson(actions.next());
                        }
                    },
                    convertUpdatedActions(updatedActions)));
        } catch (CommitFailedException e) {
            throw new io.delta.kernel.commit.CommitFailedException(
                    e.getRetryable(), e.getConflict(), e.getMessage());
        }
    }

    @Override
    public GetCommitsResponse getCommits(
            String tablePath,
            Map<String, String> tableConf,
            Long startVersion,
            Long endVersion) {
        return convertGetCommitsResponse(commitCoordinatorClient.getCommits(
                new Path(tablePath),
                tableConf,
                startVersion,
                endVersion));
    }

    @Override
    public void backfillToVersion(
            String logPath,
            Map<String, String> tableConf,
            long version,
            Long lastKnownBackfilledVersion) throws IOException {
        Path path = new Path(logPath);
        LogStore logStore = LogStoreProvider.getLogStore(hadoopConf, path.toUri().getScheme());
        commitCoordinatorClient.backfillToVersion(
                logStore,
                hadoopConf,
                path,
                tableConf,
                version,
                lastKnownBackfilledVersion);
    }

    @Override
    public Boolean semanticEquals(CommitCoordinatorClientHandler other) {
        return commitCoordinatorClient.semanticEquals(
                ((DefaultCommitCoordinatorClientHandler) other).getCommitCoordinatorClient());
    }

    public CommitCoordinatorClient getCommitCoordinatorClient() {
        return commitCoordinatorClient;
    }

    private io.delta.storage.commit.UpdatedActions convertUpdatedActions(
            UpdatedActions updatedActions) {
        if (updatedActions == null) {
            return null;
        }
        return new io.delta.storage.commit.UpdatedActions(
                convertAbstractCommitInfo(updatedActions.getCommitInfo()),
                convertAbstractMetadata(updatedActions.getNewMetadata()),
                convertAbstractProtocol(updatedActions.getNewProtocol()),
                convertAbstractMetadata(updatedActions.getOldMetadata()),
                convertAbstractProtocol(updatedActions.getOldProtocol()));
    }

    private CommitResponse convertCommitResponse(io.delta.storage.commit.CommitResponse response) {
        return new CommitResponse(convertCommit(response.getCommit()));
    }

    private Commit convertCommit(io.delta.storage.commit.Commit commit) {
        return new Commit(
                commit.getVersion(),
                convertFileStatus(commit.getFileStatus()),
                commit.getCommitTimestamp());
    }

    private FileStatus convertFileStatus(org.apache.hadoop.fs.FileStatus hadoopFileStatus) {
        return FileStatus.of(
                hadoopFileStatus.getPath().toString(),
                hadoopFileStatus.getLen(),
                hadoopFileStatus.getModificationTime());
    }

    private GetCommitsResponse convertGetCommitsResponse(
            io.delta.storage.commit.GetCommitsResponse response) {
        List<Commit> commits = response.getCommits().stream()
                .map(this::convertCommit)
                .collect(Collectors.toList());
        return new GetCommitsResponse(commits, response.getLatestTableVersion());
    }

    private io.delta.storage.commit.actions.AbstractMetadata convertAbstractMetadata(
            AbstractMetadata metadata) {
        return new io.delta.storage.commit.actions.AbstractMetadata() {
            @Override
            public String getId() {
                return metadata.getId();
            }

            @Override
            public String getName() {
                return metadata.getName();
            }

            @Override
            public String getDescription() {
                return metadata.getDescription();
            }

            @Override
            public String getProvider() {
                return metadata.getProvider();
            }

            @Override
            public Map<String, String> getFormatOptions() {
                return metadata.getFormatOptions();
            }

            @Override
            public String getSchemaString() {
                return metadata.getSchemaString();
            }

            @Override
            public List<String> getPartitionColumns() {
                return metadata.getPartitionColumns();
            }

            @Override
            public Map<String, String> getConfiguration() {
                return metadata.getConfiguration();
            }

            @Override
            public Long getCreatedTime() {
                return metadata.getCreatedTime();
            }
        };
    }

    private io.delta.storage.commit.actions.AbstractProtocol convertAbstractProtocol(
            AbstractProtocol protocol) {
        return new io.delta.storage.commit.actions.AbstractProtocol() {
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
                return protocol.getReaderFeatures();
            }

            @Override
            public Set<String> getWriterFeatures() {
                return protocol.getWriterFeatures();
            }
        };
    }

    private io.delta.storage.commit.actions.AbstractCommitInfo convertAbstractCommitInfo(
            AbstractCommitInfo commitInfo) {
        return commitInfo::getCommitTimestamp;
    }
}
