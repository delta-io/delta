/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.storage;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Default implementation of {@link LogStore} for Hadoop {@link FileSystem} implementations.
 */
public abstract class HadoopFileSystemLogStore extends LogStore {

    public HadoopFileSystemLogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    @Override
    public CloseableIterator<String> read(Path path, Configuration hadoopConf) throws IOException {
        FileSystem fs = path.getFileSystem(hadoopConf);
        FSDataInputStream stream = fs.open(path);
        Reader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        return new LineCloseableIterator(reader);
    }

    @Override
    public Iterator<FileStatus> listFrom(Path path, Configuration hadoopConf) throws IOException {
        FileSystem fs = path.getFileSystem(hadoopConf);
        if (!fs.exists(path.getParent())) {
            throw new FileNotFoundException(
                String.format("No such file or directory: %s", path.getParent())
            );
        }
        FileStatus[] files = fs.listStatus(path.getParent());
        return Arrays.stream(files)
            .filter(f -> f.getPath().getName().compareTo(path.getName()) >= 0)
            .sorted(Comparator.comparing(o -> o.getPath().getName()))
            .iterator();
    }

    @Override
    public Path resolvePathOnPhysicalStorage(
            Path path,
            Configuration hadoopConf) throws IOException {
        return path.getFileSystem(hadoopConf).makeQualified(path);
    }

    /**
     * An internal write implementation that uses FileSystem.rename().
     * <p>
     * This implementation should only be used for the underlying file systems that support atomic
     * renames, e.g., Azure is OK but HDFS is not.
     */
    protected void writeWithRename(
            Path path,
            Iterator<String> actions,
            Boolean overwrite,
            Configuration hadoopConf) throws IOException {
        FileSystem fs = path.getFileSystem(hadoopConf);

        if (!fs.exists(path.getParent())) {
            throw new FileNotFoundException(
                    String.format("No such file or directory: %s", path.getParent())
            );
        }
        if (overwrite) {
            final FSDataOutputStream stream = fs.create(path, true);
            try {
                while (actions.hasNext()) {
                    stream.write((actions.next() + "\n").getBytes(StandardCharsets.UTF_8));
                }
            } finally {
                stream.close();
            }
        } else {
            if (fs.exists(path)) {
                throw new FileAlreadyExistsException(path.toString());
            }
            Path tempPath = createTempPath(path);
            boolean streamClosed = false; // This flag is to avoid double close
            boolean renameDone = false; // This flag is to save the delete operation in most cases
            final FSDataOutputStream stream = fs.create(tempPath);
            try {
                while (actions.hasNext()) {
                    stream.write((actions.next() + "\n").getBytes(StandardCharsets.UTF_8));
                }
                stream.close();
                streamClosed = true;
                try {
                    if (fs.rename(tempPath, path)) {
                        renameDone = true;
                    } else {
                        if (fs.exists(path)) {
                            throw new FileAlreadyExistsException(path.toString());
                        } else {
                            throw new IllegalStateException(
                                    String.format("Cannot rename %s to %s", tempPath, path)
                            );
                        }
                    }
                } catch (org.apache.hadoop.fs.FileAlreadyExistsException e) {
                    throw new FileAlreadyExistsException(path.toString());
                }
            } finally {
                if (!streamClosed) {
                    stream.close();
                }
                if (!renameDone) {
                    fs.delete(tempPath, false);
                }
            }
        }
    }

    /**
     * Create a temporary path (to be used as a copy) for the input {@code path}
     */
    protected Path createTempPath(Path path) {
        return new Path(
            path.getParent(),
            String.format(".%s.%s.tmp", path.getName(), UUID.randomUUID())
        );
    }

    protected Path resolvePath(FileSystem fs, Path path) {
        return stripUserInfo(fs.makeQualified(path));
    }

    protected Path stripUserInfo(Path path) {
        final URI uri = path.toUri();

        try {
            final URI newUri = new URI(
                uri.getScheme(),
                null, // userInfo
                uri.getHost(),
                uri.getPort(),
                uri.getPath(),
                uri.getQuery(),
                uri.getFragment()
            );

            return new Path(newUri);
        } catch (URISyntaxException e) {
            // Propagating this URISyntaxException to callers would mean we would have to either
            // include it in the public LogStore.java interface or wrap it in an
            // IllegalArgumentException somewhere else. Instead, catch and wrap it here.
            throw new IllegalArgumentException(e);
        }
    }
}
