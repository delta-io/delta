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
import java.util.Optional;

import io.delta.kernel.internal.fs.Path;

import io.delta.kernel.internal.util.FileNames;

/**
 * Metadata about Delta checkpoint.
 */
public class CheckpointInstance
    implements Comparable<CheckpointInstance>
{
    /** Placeholder to identify the version that is always the latest on timeline */
    public static final CheckpointInstance MAX_VALUE = new CheckpointInstance(-1);

    public final long version;
    public final Optional<Integer> numParts;

    public CheckpointInstance(Path path)
    {
        this(FileNames.getFileVersion(path));
    }

    public CheckpointInstance(long version)
    {
        this(version, Optional.empty());
    }

    public CheckpointInstance(long version, Optional<Integer> numParts)
    {
        this.version = version;
        this.numParts = numParts;

        if (numParts.isPresent()) {
            // TODO: Add support for multi-part checkpoint file reading
            throw new UnsupportedOperationException("Reading Delta tables with mulit-part " +
                "checkpoint is not yet supported");
        }
    }

    boolean isNotLaterThan(CheckpointInstance other)
    {
        if (other == CheckpointInstance.MAX_VALUE) {
            return true;
        }
        return version <= other.version;
    }

    public List<Path> getCorrespondingFiles(Path path)
    {
        if (this == CheckpointInstance.MAX_VALUE) {
            throw new IllegalStateException("Can't get files for CheckpointVersion.MaxValue.");
        }
        return numParts
            .map(parts -> FileNames.checkpointFileWithParts(path, version, parts))
            .orElseGet(() ->
                Collections.singletonList(FileNames.checkpointFileSingular(path, version)));
    }

    @Override
    public int compareTo(CheckpointInstance that)
    {
        if (version == that.version) {
            // TODO: do we need to check for numParts when the version is matched?
            return numParts.orElse(1) - that.numParts.orElse(1);
        }
        else {
            return Long.compare(version, that.version);
        }
    }

    @Override
    public String toString()
    {
        return "CheckpointInstance{version=" + version + ", numParts=" + numParts + "}";
    }
}
