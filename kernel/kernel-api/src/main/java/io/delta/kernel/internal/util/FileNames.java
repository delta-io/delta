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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import io.delta.kernel.internal.fs.Path;

public final class FileNames
{

    private FileNames() {}

    private static final Pattern DELTA_FILE_PATTERN =
        Pattern.compile("\\d+\\.json");

    private static final Pattern CHECKPOINT_FILE_PATTERN =
        Pattern.compile("\\d+\\.checkpoint(\\.\\d+\\.\\d+)?\\.parquet");

    /**
     * Returns the delta (json format) path for a given delta file.
     */
    public static String deltaFile(Path path, long version)
    {
        return String.format("%s/%020d.json", path, version);
    }

    /**
     * Returns the version for the given delta path.
     */
    public static long deltaVersion(Path path)
    {
        return Long.parseLong(path.getName().split("\\.")[0]);
    }

    /**
     * Returns the version for the given checkpoint path.
     */
    public static long checkpointVersion(Path path)
    {
        return Long.parseLong(path.getName().split("\\.")[0]);
    }

    /**
     * Returns the prefix of all delta log files for the given version.
     * <p>
     * Intended for use with listFrom to get all files from this version onwards. The returned Path
     * will not exist as a file.
     */
    public static String listingPrefix(Path path, long version)
    {
        return String.format("%s/%020d.", path, version);
    }

    /**
     * Returns the path for a singular checkpoint up to the given version.
     * <p>
     * In a future protocol version this path will stop being written.
     */
    public static Path checkpointFileSingular(Path path, long version)
    {
        return new Path(path, String.format("%020d.checkpoint.parquet", version));
    }

    /**
     * Returns the paths for all parts of the checkpoint up to the given version.
     * <p>
     * In a future protocol version we will write this path instead of checkpointFileSingular.
     * <p>
     * Example of the format: 00000000000000004915.checkpoint.0000000020.0000000060.parquet is
     * checkpoint part 20 out of 60 for the snapshot at version 4915. Zero padding is for
     * lexicographic sorting.
     */
    public static List<Path> checkpointFileWithParts(Path path, long version, int numParts)
    {
        final List<Path> output = new ArrayList<>();
        for (int i = 1; i < numParts + 1; i++) {
            output.add(
                new Path(
                    path,
                    String.format(
                        "%020d.checkpoint.%010d.%010d.parquet", i, numParts, version)
                )
            );
        }
        return output;
    }

    public static boolean isCheckpointFile(String fileName)
    {
        return CHECKPOINT_FILE_PATTERN.matcher(fileName).find();
    }

    public static boolean isCommitFile(String fileName)
    {
        return DELTA_FILE_PATTERN.matcher(fileName).find();
    }

    /**
     * Get the version of the checkpoint, checksum or delta file. Throws an error if an unexpected
     * file type is seen. These unexpected files should be filtered out to ensure forward
     * compatibility in cases where new file types are added, but without an explicit protocol
     * upgrade.
     */
    public static long getFileVersion(Path path)
    {
        if (isCheckpointFile(path.getName())) {
            return checkpointVersion(path);
        }
        else if (isCommitFile(path.getName())) {
            return deltaVersion(path);
            //} else if (isChecksumFile(path)) {
            //    checksumVersion(path);
        }
        else {
            throw new IllegalArgumentException(
                String.format("Unexpected file type found in transaction log: %s", path)
            );
        }
    }
}
