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
package io.delta.kernel.defaults.internal.coordinatedcommits;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.delta.kernel.engine.coordinatedcommits.Commit;
import io.delta.kernel.engine.coordinatedcommits.CommitResponse;
import io.delta.kernel.engine.coordinatedcommits.GetCommitsResponse;
import io.delta.kernel.engine.coordinatedcommits.UpdatedActions;
import io.delta.kernel.engine.coordinatedcommits.actions.AbstractCommitInfo;
import io.delta.kernel.engine.coordinatedcommits.actions.AbstractMetadata;
import io.delta.kernel.engine.coordinatedcommits.actions.AbstractProtocol;
import io.delta.kernel.utils.FileStatus;

public class StorageKernelAPIAdapter {
    public static io.delta.storage.commit.UpdatedActions toStorageUpdatedActions(
            UpdatedActions updatedActions) {
        if (updatedActions == null) {
            return null;
        }
        return new io.delta.storage.commit.UpdatedActions(
                toStorageAbstractCommitInfo(updatedActions.getCommitInfo()),
                toStorageAbstractMetadata(updatedActions.getNewMetadata()),
                toStorageAbstractProtocol(updatedActions.getNewProtocol()),
                toStorageAbstractMetadata(updatedActions.getOldMetadata()),
                toStorageAbstractProtocol(updatedActions.getOldProtocol()));
    }

    public static CommitResponse toKernelAPICommitResponse(
            io.delta.storage.commit.CommitResponse response) {
        return new CommitResponse(toKernelAPICommit(response.getCommit()));
    }

    public static Commit toKernelAPICommit(io.delta.storage.commit.Commit commit) {
        return new Commit(
                commit.getVersion(),
                toKernelAPIFileStatus(commit.getFileStatus()),
                commit.getCommitTimestamp());
    }

    public static FileStatus toKernelAPIFileStatus(
            org.apache.hadoop.fs.FileStatus hadoopFileStatus) {
        return FileStatus.of(
                hadoopFileStatus.getPath().toString(),
                hadoopFileStatus.getLen(),
                hadoopFileStatus.getModificationTime());
    }

    public static GetCommitsResponse toKernelAPIGetCommitsResponse(
            io.delta.storage.commit.GetCommitsResponse response) {
        List<Commit> commits = response.getCommits().stream()
                .map(StorageKernelAPIAdapter::toKernelAPICommit)
                .collect(Collectors.toList());
        return new GetCommitsResponse(commits, response.getLatestTableVersion());
    }

    public static io.delta.storage.commit.actions.AbstractMetadata toStorageAbstractMetadata(
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
                return metadata.getCreatedTime().orElse(null);
            }
        };
    }

    public static io.delta.storage.commit.actions.AbstractProtocol toStorageAbstractProtocol(
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

    public static io.delta.storage.commit.actions.AbstractCommitInfo toStorageAbstractCommitInfo(
            AbstractCommitInfo commitInfo) {
        return commitInfo::getCommitTimestamp;
    }
}
