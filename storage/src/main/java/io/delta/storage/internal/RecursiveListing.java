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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;

import io.delta.storage.ListFilter;
import io.delta.storage.LogStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * Default implementation of {@link LogStore#listFrom(Path, boolean, ListFilter, Configuration)}
 * on top of the single-directory {@link LogStore#listFrom(Path, Configuration)}.
 *
 * <p>Lazy: a directory is listed only when the iterator reaches it, and directories are returned
 * before their contents (pre-order).</p>
 */
public final class RecursiveListing {

    /** Name that sorts before every real file name; used to list a whole directory. */
    public static final String LIST_ALL = "\u0000";

    private RecursiveListing() {}

    public static Iterator<FileStatus> listFrom(
            LogStore store,
            Path path,
            boolean recursive,
            ListFilter filter,
            Configuration hadoopConf) throws IOException {
        final Iterator<FileStatus> top = filtered(store.listFrom(path, hadoopConf), filter);
        return recursive ? new PreOrderIterator(store, top, filter, hadoopConf) : top;
    }

    /** Drops entries the filter rejects. */
    static Iterator<FileStatus> filtered(Iterator<FileStatus> it, ListFilter filter) {
        if (filter == ListFilter.NONE) {
            return it;
        }
        return new Iterator<FileStatus>() {
            private FileStatus next = advance();

            private FileStatus advance() {
                while (it.hasNext()) {
                    final FileStatus s = it.next();
                    final String name = s.getPath().getName();
                    final boolean skip =
                        s.isDirectory() ? filter.skipDirectory(name) : filter.skipFile(name);
                    if (!skip) {
                        return s;
                    }
                }
                return null;
            }

            @Override
            public boolean hasNext() { return next != null; }

            @Override
            public FileStatus next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                final FileStatus result = next;
                next = advance();
                return result;
            }
        };
    }

    private static final class PreOrderIterator implements Iterator<FileStatus> {
        private final LogStore store;
        private final ListFilter filter;
        private final Configuration hadoopConf;
        private final Deque<Iterator<FileStatus>> stack = new ArrayDeque<>();

        PreOrderIterator(
                LogStore store,
                Iterator<FileStatus> top,
                ListFilter filter,
                Configuration hadoopConf) {
            this.store = store;
            this.filter = filter;
            this.hadoopConf = hadoopConf;
            stack.push(top);
        }

        @Override
        public boolean hasNext() {
            while (!stack.isEmpty()) {
                if (stack.peek().hasNext()) {
                    return true;
                }
                stack.pop();
            }
            return false;
        }

        @Override
        public FileStatus next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final FileStatus status = stack.peek().next();
            if (status.isDirectory()) {
                stack.push(children(status.getPath()));
            }
            return status;
        }

        private Iterator<FileStatus> children(Path dir) {
            try {
                return filtered(store.listFrom(new Path(dir, LIST_ALL), hadoopConf), filter);
            } catch (FileNotFoundException e) {
                // Deleted while we were listing (e.g. a concurrent VACUUM): nothing to return.
                return Collections.emptyIterator();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
