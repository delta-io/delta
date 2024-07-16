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
package io.delta.kernel.defaults.internal.coordinatedcommits;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

import io.delta.storage.LogStore;
import io.delta.storage.commit.actions.AbstractCommitInfo;
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.VectorUtils;
import static io.delta.kernel.internal.TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME;

public class CoordinatedCommitsDefaultUtils {

    /**
     * Write a UUID-based commit file for the specified version to the table at logPath.
     */
    public static FileStatus writeCommitFile(
            LogStore logStore,
            Configuration hadoopConf,
            String logPath,
            long commitVersion,
            Iterator<String> actions,
            String uuid) throws IOException {
        Path commitPath = new Path(
                FileNames.unbackfilledDeltaFile(
                        new io.delta.kernel.internal.fs.Path(logPath),
                        commitVersion,
                        Optional.of(uuid)).toString());
        FileSystem fs = commitPath.getFileSystem(hadoopConf);
        if (!fs.exists(commitPath.getParent())) {
            fs.mkdirs(commitPath.getParent());
        }
        logStore.write(commitPath, actions, false, hadoopConf);
        return commitPath.getFileSystem(hadoopConf).getFileStatus(commitPath);
    }

    /**
     * Get the table path from the provided log path.
     */
    public static Path getTablePath(Path logPath) {
        return logPath.getParent();
    }

    /**
     * Helper method to recover the saved value of `tableConfig` from `abstractMetadata`.
     * Return defaultValue if the key is not in the configuration.
     */
    public static <T> T fromAbstractMetadataAndTableConfig(
            AbstractMetadata abstractMetadata, TableConfig<T> tableConfig) {
        Map<String, String> conf = abstractMetadata.getConfiguration();
        String value = conf.getOrDefault(tableConfig.getKey(), tableConfig.getDefaultValue());
        Function<String, T> fromString = tableConfig.getFromString();
        return fromString.apply(value);
    }

    /**
     * Get the commit coordinator name from the provided abstract metadata.
     */
    public static Optional<String> getCommitCoordinatorName(AbstractMetadata abstractMetadata) {
        return fromAbstractMetadataAndTableConfig(
                abstractMetadata, COORDINATED_COMMITS_COORDINATOR_NAME);
    }

    /**
     * Get the hadoop file path for the delta file for the specified version.
     *
     * @param logPath The root path of the delta log.
     * @param version The version of the delta file.
     * @return The hadoop file path for the delta file.
     */
    public static Path getHadoopDeltaFile(Path logPath, long version) {
        return new Path(FileNames
                .deltaFile(new io.delta.kernel.internal.fs.Path(logPath.toString()), version));
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
