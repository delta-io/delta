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
package io.delta.kernel.internal.checkpoint;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.delta.kernel.fs.Path;
import io.delta.kernel.internal.lang.Ordered;
import io.delta.kernel.internal.util.FileNames;

public class CheckpointInstance
        implements Ordered<CheckpointInstance>
{
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
        assert (this != CheckpointInstance.MAX_VALUE) :
                "Can't get files for CheckpointVersion.MaxValue.";
        return numParts
                .map(parts -> FileNames.checkpointFileWithParts(path, version, parts))
                .orElseGet(() ->
                        Collections.singletonList(FileNames.checkpointFileSingular(path, version)));
    }

    @Override
    public int compareTo(CheckpointInstance that)
    {
        if (version == that.version) {
            return numParts.orElse(1) - that.numParts.orElse(1);
        }
        else {
            // we need to guard against overflow. We just can't return (this - that).toInt
            return version - that.version < 0 ? -1 : 1;
        }
    }

    @Override
    public String toString()
    {
        return "CheckpointInstance{" +
                "version=" + version +
                ", numParts=" + numParts +
                '}';
    }
}
