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

package io.delta.kernel.utils;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import io.delta.kernel.annotation.Evolving;

/**
 * Class for encapsulating metadata about a file in Delta Lake table.
 *
 * @since 3.0.0
 */
@Evolving
public class FileStatus {
    private final String path;
    private final long size;
    private final long modificationTime;
    private final Map<String, String> tags;

    // TODO add further documentation about the expected format for modificationTime?
    protected FileStatus(
        String path,
        long size,
        long modificationTime,
        Map<String, String> tags) {
        this.path = Objects.requireNonNull(path, "path is null");
        this.size = size; // TODO: validation
        this.modificationTime = modificationTime; // TODO: validation
        this.tags = tags;
    }

    /**
     * Get the path to the file.
     *
     * @return Fully qualified file path
     */
    public String getPath() {
        return path;
    }

    /**
     * Get the size of the file in bytes.
     *
     * @return File size in bytes.
     */
    public long getSize() {
        return size;
    }

    /**
     * Get the modification time of the file in epoch millis.
     *
     * @return Modification time in epoch millis
     */
    public long getModificationTime() {
        return modificationTime;
    }

    /**
     * Get the tags of the file.
     *
     * @return the file tags
     */
    public Map<String, String> getTags() {
        return tags;
    }

    /**
     * Create a {@link FileStatus} with the given path, size and modification time.
     *
     * @param path Fully qualified file path.
     * @param size File size in bytes
     * @param modificationTime Modification time of the file in epoch millis
     */
    public static FileStatus of(String path, long size, long modificationTime) {
        return new FileStatus(path, size, modificationTime, Collections.emptyMap());
    }

    /**
     * Create a {@link FileStatus} with the given path, size, modification time and tags.
     *
     * @param path Fully qualified file path.
     * @param size File size in bytes
     * @param modificationTime Modification time of the file in epoch millis
     * @param tags File tags
     */
    public static FileStatus of(String path, long size, long modificationTime, Map<String, String> tags) {
        return new FileStatus(path, size, modificationTime, tags);
    }

}
