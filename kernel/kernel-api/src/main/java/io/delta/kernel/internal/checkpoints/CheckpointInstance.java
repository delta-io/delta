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
package io.delta.kernel.internal.checkpoints;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;

/**
 * Metadata about Delta checkpoint.
 */
public class CheckpointInstance
    implements Comparable<CheckpointInstance> {
    /**
     * Placeholder to identify the version that is always the latest on timeline
     */
    public static final CheckpointInstance MAX_VALUE = new CheckpointInstance(Long.MAX_VALUE);

    public final long version;
    public final Optional<Integer> numParts;

    public final CheckpointFormat format;

    public final Optional<Path> filePath;

    public CheckpointInstance(String path) {
        String[] pathParts = getPathName(path).split("\\.");
        this.filePath = Optional.of(new Path(path));

        if (pathParts.length == 3 && pathParts[1].equals("checkpoint") &&
                pathParts[2].equals("parquet")) {
            // Classic checkpoint 00000000000000000010.checkpoint.parquet
            this.version = Long.parseLong(pathParts[0]);
            this.numParts = Optional.empty();
            this.format = new SingleFormat();
        } else if (pathParts.length == 5 && pathParts[1].equals("checkpoint")
                && pathParts[4].equals("parquet")) {
            // Multi-part checkpoint 00000000000000000010.checkpoint.0000000001.0000000003.parquet
            this.version = Long.parseLong(pathParts[0]);
            this.numParts = Optional.of(Integer.parseInt(pathParts[3]));
            this.format = new MultipartFormat();
        } else if (pathParts.length == 4 && pathParts[1].equals("checkpoint")
                && (pathParts[3].equals("parquet") || pathParts[3].equals("json"))) {
            // V2 checkpoint 00000000000000000010.checkpoint.UUID.(parquet|json)
            this.version = Long.parseLong(pathParts[0]);
            this.numParts = Optional.empty();
            this.format = new V2Format();
        } else {
            throw new RuntimeException("Unrecognized checkpoint path format: " + getPathName(path));
        }
    }

    public CheckpointInstance(long version) {
        this(version, Optional.empty());
    }

    public CheckpointInstance(long version, Optional<Integer> numParts) {
        this.version = version;
        this.numParts = numParts;
        this.filePath = Optional.empty();
        if (numParts.orElse(0) == 0) {
            this.format = new SingleFormat();
        } else {
            this.format = new MultipartFormat();
        }
    }

    boolean isNotLaterThan(CheckpointInstance other) {
        if (other == CheckpointInstance.MAX_VALUE) {
            return true;
        }
        return version <= other.version;
    }

    public List<Path> getReferencedSidecars(TableClient tableClient, Path logPath) {
        return new Checkpointer(logPath).loadSidecarFiles(tableClient, filePath.get())
                .orElse(Collections.emptyList())
                .stream()
                .map(sidecar -> new Path(sidecar.path))
                .collect(Collectors.toList());
    }

    boolean isEarlierThan(CheckpointInstance other) {
        if (other == CheckpointInstance.MAX_VALUE) {
            return true;
        }
        return version < other.version;
    }

    public List<Path> getCorrespondingFiles(Path path) {
        if (this == CheckpointInstance.MAX_VALUE) {
            throw new IllegalStateException("Can't get files for CheckpointVersion.MaxValue.");
        }

        if (format instanceof V2Format) {
            return Collections.singletonList(filePath.get());
        }

        return numParts
                .map(parts -> FileNames.checkpointFileWithParts(path, version, parts))
                .orElseGet(() ->
                        Collections.singletonList(
                                FileNames.checkpointFileSingular(path, version)));
    }

    /**
     * Comparison rules:
     * 1. A CheckpointInstance with higher version is greater than the one with lower version.
     * 2. For CheckpointInstances with same version, a Multi-part checkpoint is greater than a
     *    Single part checkpoint.
     * 3. For Multi-part CheckpointInstance corresponding to same version, the one with more
     *    parts is greater than the one with less parts.
     */
    @Override
    public int compareTo(CheckpointInstance that) {
        if (version == that.version) {
            return Long.compare(numParts.orElse(1), that.numParts.orElse(1));
        } else {
            return Long.compare(version, that.version);
        }
    }

    @Override
    public String toString() {
        return "CheckpointInstance{version=" + version + ", numParts=" + numParts + ", format" +
                format.name + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CheckpointInstance checkpointInstance = (CheckpointInstance) o;
        return version == checkpointInstance.version &&
                Objects.equals(numParts, checkpointInstance.numParts) &&
                format.name.equals(checkpointInstance.format.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, numParts, format.name);
    }

    private String getPathName(String path) {
        int slash = path.lastIndexOf("/");
        return path.substring(slash + 1);
    }
}
