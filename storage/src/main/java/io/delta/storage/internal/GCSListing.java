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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import io.delta.storage.ListFilter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Server-side {@code listFrom} for GCS.
 *
 * <p>Built on one primitive: a flat, lexicographic listing of object keys starting at a key
 * (inclusive), one page at a time. The gcs-connector exposes it as
 * {@code GoogleHadoopFileSystem.listStatusStartingFrom(Path)} (gcs-connector 4.0.4+), backed by
 * the {@code startOffset} parameter of the GCS {@code objects.list} API.</p>
 *
 * <p>Object keys under a directory are contiguous in that order, so:</p>
 * <ul>
 *   <li>The listing stops at the first key outside {@code <dir>/}.</li>
 *   <li>A whole subtree {@code <dir>/<sub>/...} is skipped with one request by restarting at
 *       {@code <dir>/<sub>0}: {@code '0'} is the character right after {@code '/'}.</li>
 *   <li>The next page starts at the last key plus {@code '\u0000'}, its immediate successor, so
 *       exactly the already-listed key is excluded.</li>
 * </ul>
 *
 * <p>Results match {@code FileSystem.listStatus}-based listing (files and directories, start-name
 * bound at the top level, filter at every depth) except for order in one corner case: entries come
 * back in object-key order, where a directory {@code d} sorts as {@code d/}. So a file named
 * {@code d.x} (any character below {@code '/'} after the shared prefix) is returned before the
 * directory {@code d}, while a name sort puts {@code d} first. Delta's callers don't depend on the
 * relative order of a directory and such a file.</p>
 */
public final class GCSListing {

    /** One page of a flat key listing starting at {@code start} (inclusive). */
    @FunctionalInterface
    public interface PageSource {
        /**
         * Returns the next keys at or after {@code start}, in key order, across the whole bucket.
         * An empty result means there is nothing at or after {@code start}.
         */
        FileStatus[] listStartingFrom(Path start) throws IOException;
    }

    private static final String CONNECTOR_METHOD = "listStatusStartingFrom";

    private static final ConcurrentHashMap<Class<?>, Optional<Method>> METHODS =
        new ConcurrentHashMap<>();

    private GCSListing() {}

    /**
     * Returns a {@link PageSource} backed by the connector's {@code listStatusStartingFrom}, or
     * empty if {@code fs} (after unwrapping {@link FilterFileSystem}s) doesn't have it, for example
     * an older gcs-connector or a different {@code gs://} implementation.
     *
     * <p>Looked up by reflection so delta-storage needs no compile-time dependency on the
     * connector. The listing runs on the same FileSystem instance, so it uses the same credentials
     * and configuration as every other LogStore operation.</p>
     */
    public static Optional<PageSource> forFileSystem(FileSystem fs) {
        FileSystem target = fs;
        while (methodOf(target.getClass()) == null && target instanceof FilterFileSystem) {
            target = ((FilterFileSystem) target).getRawFileSystem();
        }
        final Method method = methodOf(target.getClass());
        if (method == null) {
            return Optional.empty();
        }
        final FileSystem connector = target;
        return Optional.of(start -> invoke(method, connector, start));
    }

    private static Method methodOf(Class<?> cls) {
        return METHODS.computeIfAbsent(cls, c -> {
            try {
                final Method m = c.getMethod(CONNECTOR_METHOD, Path.class);
                return FileStatus[].class.isAssignableFrom(m.getReturnType())
                    ? Optional.of(m) : Optional.empty();
            } catch (NoSuchMethodException | LinkageError e) {
                return Optional.empty();
            }
        }).orElse(null);
    }

    private static FileStatus[] invoke(Method method, FileSystem fs, Path start)
            throws IOException {
        try {
            return (FileStatus[]) method.invoke(fs, start);
        } catch (InvocationTargetException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IOException(cause);
        } catch (IllegalAccessException e) {
            throw new IOException(e);
        }
    }

    /**
     * Lists {@code resolvedPath.getParent()} from {@code resolvedPath.getName()}, with the
     * semantics of {@code LogStore.listFrom(Path, boolean, ListFilter, Configuration)}.
     *
     * <p>The caller is responsible for the missing-directory check: a flat listing of a missing
     * directory is simply empty.</p>
     */
    public static Iterator<FileStatus> listFrom(
            PageSource source,
            Path resolvedPath,
            boolean recursive,
            ListFilter filter) {
        return new KeyOrderIterator(source, resolvedPath, recursive, filter);
    }

    /** Object key of {@code path}: the decoded URI path without the leading slash. */
    static String keyOf(Path path) {
        final String p = path.toUri().getPath();
        return p.startsWith("/") ? p.substring(1) : p;
    }

    private static final class KeyOrderIterator implements Iterator<FileStatus> {
        private final PageSource source;
        private final String scheme;
        private final String authority;
        private final String dirKey;      // "<dir>/", or "" at the bucket root
        private final String startName;   // top-level lower bound (inclusive)
        private final boolean recursive;
        private final ListFilter filter;

        private final ArrayDeque<FileStatus> ready = new ArrayDeque<>();
        /** Recursive mode: relative paths of the directories returned on the current branch. */
        private final List<String> openDirs = new ArrayList<>();
        /** Where the next page starts, or null when the listing is done. */
        private String nextStart;

        KeyOrderIterator(PageSource source, Path resolvedPath, boolean recursive, ListFilter filter) {
            final URI uri = resolvedPath.toUri();
            this.source = source;
            this.scheme = uri.getScheme();
            this.authority = uri.getAuthority();
            final String parentKey = keyOf(resolvedPath.getParent());
            this.dirKey = parentKey.isEmpty() ? "" : parentKey + "/";
            this.startName = resolvedPath.getName();
            this.recursive = recursive;
            this.filter = filter;
            this.nextStart = dirKey + startName;
        }

        @Override
        public boolean hasNext() {
            while (ready.isEmpty() && nextStart != null) {
                try {
                    fetchPage();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            return !ready.isEmpty();
        }

        @Override
        public FileStatus next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return ready.poll();
        }

        private Path pathOf(String key) {
            return new Path(scheme, authority, "/" + key);
        }

        private FileStatus directory(String rel) {
            return new FileStatus(0L, true, 0, 0L, 0L, pathOf(dirKey + rel));
        }

        private void fetchPage() throws IOException {
            final String start = nextStart;
            final FileStatus[] page = source.listStartingFrom(pathOf(start));
            if (page.length == 0) {
                nextStart = null;
                return;
            }
            String lastKey = null;
            for (FileStatus status : page) {
                final String rawKey = keyOf(status.getPath());
                final boolean placeholder = status.isDirectory() || rawKey.endsWith("/");
                final String key =
                    rawKey.endsWith("/") ? rawKey.substring(0, rawKey.length() - 1) : rawKey;
                if ((key + "/").equals(dirKey)) {
                    continue; // the listed directory's own placeholder
                }
                if (!key.startsWith(dirKey)) {
                    // Keys are in order, so nothing after this is inside the directory.
                    nextStart = null;
                    return;
                }
                // Resume from the object's real key. A directory placeholder is stored as
                // "<key>/", but Hadoop's Path drops the trailing slash, and "<key>/" sorts after
                // "<key>\u0000": resuming from the slashless key would return it again forever.
                lastKey = placeholder ? key + "/" : rawKey;
                final String rel = key.substring(dirKey.length());
                if (rel.isEmpty()) {
                    continue;
                }
                final String resume = recursive
                    ? acceptRecursive(rel, status, placeholder)
                    : acceptDirectChild(rel, status, placeholder);
                if (resume != null) {
                    nextStart = resume; // skip a subtree: drop the rest of this page
                    checkProgress(start);
                    return;
                }
            }
            nextStart = lastKey == null ? null : lastKey + "\u0000";
            checkProgress(start);
        }

        /** Every request must start strictly after the previous one, or the listing could loop. */
        private void checkProgress(String previousStart) {
            if (nextStart != null && nextStart.compareTo(previousStart) <= 0) {
                throw new IllegalStateException(String.format(
                    "GCS listing of %s made no progress: next start %s is not after %s",
                    dirKey, nextStart, previousStart));
            }
        }

        /** Non-recursive. Returns where to restart, or null to keep reading this page. */
        private String acceptDirectChild(String rel, FileStatus status, boolean placeholder) {
            final int slash = rel.indexOf('/');
            if (slash < 0 && !placeholder) {
                if (rel.compareTo(startName) >= 0 && !filter.skipFile(rel)) {
                    ready.add(status);
                }
                return null;
            }
            final String sub = slash < 0 ? rel : rel.substring(0, slash);
            if (sub.compareTo(startName) >= 0 && !filter.skipDirectory(sub)) {
                ready.add(directory(sub));
            }
            return dirKey + sub + "0";
        }

        /** Recursive. Returns where to restart, or null to keep reading this page. */
        private String acceptRecursive(String rel, FileStatus status, boolean placeholder) {
            final String[] parts = rel.split("/", -1);
            final int dirDepth = placeholder ? parts.length : parts.length - 1;

            // The start name bounds the top level only, as in the default implementation.
            if (parts[0].compareTo(startName) < 0) {
                return dirDepth > 0 ? dirKey + parts[0] + "0" : null;
            }

            String dirRel = null;
            for (int i = 0; i < dirDepth; i++) {
                dirRel = i == 0 ? parts[0] : dirRel + "/" + parts[i];
                if (i < openDirs.size() && openDirs.get(i).equals(dirRel)) {
                    continue; // already returned on this branch
                }
                truncateOpenDirs(i);
                if (filter.skipDirectory(parts[i])) {
                    return dirKey + dirRel + "0";
                }
                ready.add(directory(dirRel));
                openDirs.add(dirRel);
            }
            truncateOpenDirs(dirDepth);
            if (!placeholder && !filter.skipFile(parts[parts.length - 1])) {
                ready.add(status);
            }
            return null;
        }

        private void truncateOpenDirs(int size) {
            while (openDirs.size() > size) {
                openDirs.remove(openDirs.size() - 1);
            }
        }
    }
}
