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
package io.delta.kernel.internal.util;


import java.util.*;

import io.delta.kernel.commit.actions.AbstractCommitInfo;
import io.delta.kernel.commit.actions.AbstractMetadata;
import io.delta.kernel.commit.actions.AbstractProtocol;
import io.delta.kernel.engine.CommitCoordinatorClientHandler;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import static io.delta.kernel.internal.TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF;
import static io.delta.kernel.internal.TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME;

public class CoordinatedCommitsUtils {
    public static Optional<CommitCoordinatorClientHandler> getCommitCoordinatorClientHandler(
            Engine engine, Metadata metadata, Protocol protocol) {
        return COORDINATED_COMMITS_COORDINATOR_NAME.fromMetadata(metadata)
                .map(commitCoordinatorStr -> engine.getCommitCoordinatorClientHandler(
                commitCoordinatorStr,
                COORDINATED_COMMITS_COORDINATOR_CONF.fromMetadata(metadata)));
    }

    public static Tuple2<Optional<String>, Map<String, String>> getCoordinatedCommitsConfs(
            Metadata metadata) {
        Optional<String> coordinatorName = COORDINATED_COMMITS_COORDINATOR_NAME
                .fromMetadata(metadata);
        Map<String, String> coordinatorConf;
        if (coordinatorName.isPresent()) {
            coordinatorConf = COORDINATED_COMMITS_COORDINATOR_CONF.fromMetadata(metadata);
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
            public Long getCreatedTime() {
                return metadata.getCreatedTime().orElse(null);
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
}
