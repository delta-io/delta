/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.storage.internal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Static utility methods for the GCSLogStore fast listFrom path.
 *
 * References to gcs-connector classes are isolated in this class (like S3LogStoreUtil for
 * hadoop-aws) so that GCSLogStore always loads and can catch {@link LinkageError} to fall back
 * to the default listFrom when the connector does not provide the required API.
 *
 * The fast path uses {@code GoogleHadoopFileSystem.listStatusStartingFrom} (gcs-connector
 * 4.0.4+), which pushes the GCS objects.list startOffset parameter server-side through the same
 * FileSystem instance, and therefore the same credentials and configuration, as all other
 * LogStore operations. The method is not relocated in the connector's shaded jar and only uses
 * Hadoop types in its signature, so no unshaded Google classes are needed on the classpath.
 */
public final class GCSLogStoreUtil {
    private GCSLogStoreUtil() {}

    /**
     * Unwraps {@link FilterFileSystem} wrappers that delegate to a real
     * {@link GoogleHadoopFileSystem}.
     */
    static FileSystem unwrap(FileSystem fs) {
        while (!(fs instanceof GoogleHadoopFileSystem) && fs instanceof FilterFileSystem) {
            fs = ((FilterFileSystem) fs).getRawFileSystem();
        }
        return fs;
    }

    /**
     * Whether the fast list path applies to {@code fs}: true only when it is (or wraps) a
     * {@link GoogleHadoopFileSystem}. Throws {@link LinkageError} when the gcs-connector entry
     * class is missing from the classpath; callers catch it and fall back.
     */
    public static boolean isGoogleHadoopFileSystem(FileSystem fs) {
        return unwrap(fs) instanceof GoogleHadoopFileSystem;
    }

    /**
     * Lists files in {@code parentPath} which are lexicographically equal to or after
     * {@code resolvedPath}, ordered by name, using server-side startOffset pushdown:
     * O(entries returned) instead of the O(directory size) {@code fs.listStatus} in
     * {@code HadoopFileSystemLogStore.listFrom}. The returned iterator holds at most one list
     * page; the first page is fetched eagerly so IO, auth and linkage problems surface here,
     * later pages wrap their {@link IOException} in {@link UncheckedIOException}.
     *
     * @throws UnsupportedOperationException when {@code fs} is not (and does not wrap) a
     *         {@link GoogleHadoopFileSystem}, or {@code parentPath} is a bucket root.
     */
    public static Iterator<FileStatus> gcsListFrom(
            FileSystem fs,
            Path resolvedPath,
            Path parentPath) throws IOException {
        final FileSystem unwrapped = unwrap(fs);
        if (!(unwrapped instanceof GoogleHadoopFileSystem)) {
            throw new UnsupportedOperationException(
                "The fast GCS list path (delta.enableFastGCSListFrom) requires the file system " +
                    "to be a com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem. Got: " +
                    fs.getClass().getName());
        }
        final PagedListIterator iterator = new PagedListIterator(
            (GoogleHadoopFileSystem) unwrapped, resolvedPath, parentPath);
        iterator.ensureLoaded();
        return iterator;
    }

    /**
     * Object key of {@code path} within its bucket, i.e. the URI path without the
     * leading slash.
     */
    static String pathToKey(Path path) {
        final String key = path.toUri().getPath();
        return key.startsWith("/") ? key.substring(1) : key;
    }

    /**
     * Lazily pages through {@code GoogleHadoopFileSystem.listStatusStartingFrom} results,
     * holding at most one page in memory.
     */
    private static final class PagedListIterator implements Iterator<FileStatus> {
        private final GoogleHadoopFileSystem ghfs;
        /** Parent directory object key, including the trailing slash. */
        private final String dirKey;

        private final ArrayDeque<FileStatus> buffer = new ArrayDeque<>();
        /** Inclusive start of the next page; null once the listing is exhausted. */
        private Path nextPageStart;

        PagedListIterator(GoogleHadoopFileSystem ghfs, Path resolvedPath, Path parentPath) {
            final String parentKey = pathToKey(parentPath);
            if (parentKey.isEmpty()) {
                throw new UnsupportedOperationException(
                    "The fast GCS list path does not support listing from a bucket root: " +
                        resolvedPath);
            }
            this.ghfs = ghfs;
            this.dirKey = parentKey.endsWith("/") ? parentKey : parentKey + "/";
            this.nextPageStart = resolvedPath;
        }

        @Override
        public boolean hasNext() {
            try {
                ensureLoaded();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return !buffer.isEmpty();
        }

        @Override
        public FileStatus next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return buffer.poll();
        }

        void ensureLoaded() throws IOException {
            while (buffer.isEmpty() && nextPageStart != null) {
                fetchPage();
            }
        }

        private void fetchPage() throws IOException {
            final FileStatus[] page = ghfs.listStatusStartingFrom(nextPageStart);
            if (page.length == 0) {
                // The connector pages past all-placeholder raw pages internally, so an empty
                // result means the listing is done.
                nextPageStart = null;
                return;
            }
            FileStatus lastRawStatus = null;
            for (FileStatus status : page) {
                final String key = pathToKey(status.getPath());
                if (!key.startsWith(dirKey)) {
                    // The flat listing has left the parent directory. Keys are returned in
                    // lexicographic order, so nothing after this can be inside it: done.
                    nextPageStart = null;
                    return;
                }
                lastRawStatus = status;
                if (status.isDirectory() || key.indexOf('/', dirKey.length()) >= 0) {
                    // Only direct children, like fs.listStatus: skip entries under
                    // subdirectories of the parent (e.g. _delta_log/_staged_commits/*).
                    continue;
                }
                buffer.add(status);
            }
            // Resume from the immediate lexicographic successor of the last raw key (key + NUL):
            // startOffset is inclusive, so this excludes exactly the already-listed key and can
            // skip no other. A short page is NOT an end-of-listing signal (the connector filters
            // directory placeholders out of the raw page), so only an empty page or leaving the
            // parent directory ends the listing.
            nextPageStart = new Path(lastRawStatus.getPath().toString() + "\0");
        }
    }
}
