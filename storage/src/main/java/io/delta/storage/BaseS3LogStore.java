/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

import com.google.common.io.CountingOutputStream;
import io.delta.storage.internal.PathLock;
import io.delta.storage.internal.S3LogStoreUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

/** Shared S3 path, listing, and direct-write behavior for S3 LogStore implementations. */
abstract class BaseS3LogStore extends HadoopFileSystemLogStore {

    private static final PathLock PATH_LOCK = new PathLock();

    private final boolean enableFastListFrom =
        initHadoopConf().getBoolean("delta.enableFastS3AListFrom", false);

    BaseS3LogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    static Path resolvePathWithoutUserInfo(FileSystem fs, Path path) {
        return stripUserInfo(fs.makeQualified(path));
    }

    private static Path stripUserInfo(Path path) {
        final URI uri = path.toUri();
        try {
            return new Path(new URI(
                uri.getScheme(),
                null,
                uri.getHost(),
                uri.getPort(),
                uri.getPath(),
                uri.getQuery(),
                uri.getFragment()));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    protected final void writeWithPathLock(
            Path path,
            Iterator<String> actions,
            boolean overwrite,
            Configuration hadoopConf) throws IOException {
        final FileSystem fs = path.getFileSystem(hadoopConf);
        final Path resolvedPath = resolvePathWithoutUserInfo(fs, path);
        try {
            PATH_LOCK.acquire(resolvedPath);
            try {
                if (fs.exists(resolvedPath) && !overwrite) {
                    throw new java.nio.file.FileAlreadyExistsException(
                        resolvedPath.toUri().toString());
                }

                final CountingOutputStream stream =
                    new CountingOutputStream(fs.create(resolvedPath, overwrite));
                while (actions.hasNext()) {
                    stream.write((actions.next() + "\n").getBytes(StandardCharsets.UTF_8));
                }
                stream.close();
            } catch (org.apache.hadoop.fs.FileAlreadyExistsException e) {
                throw new java.nio.file.FileAlreadyExistsException(e.getMessage());
            }
        } catch (InterruptedException e) {
            throw new InterruptedIOException(e.getMessage());
        } finally {
            PATH_LOCK.release(resolvedPath);
        }
    }

    @Override
    public Iterator<FileStatus> listFrom(Path path, Configuration hadoopConf) throws IOException {
        final FileSystem fs = path.getFileSystem(hadoopConf);
        final Path resolvedPath = resolvePathWithoutUserInfo(fs, path);
        final Path parentPath = resolvedPath.getParent();

        final FileStatus[] statuses;
        if (fs instanceof LocalFileSystem
                || fs instanceof RawLocalFileSystem
                || !enableFastListFrom) {
            statuses = fs.listStatus(parentPath);
        } else {
            if (!fs.exists(parentPath)) {
                throw new FileNotFoundException(
                    String.format("No such file or directory: %s", parentPath));
            }
            statuses = S3LogStoreUtil.s3ListFromArray(fs, resolvedPath, parentPath);
        }

        return Arrays.stream(statuses)
            .filter(status -> status.getPath().getName().compareTo(resolvedPath.getName()) >= 0)
            .sorted(Comparator.comparing(status -> status.getPath().getName()))
            .iterator();
    }

    @Override
    public Boolean isPartialWriteVisible(Path path, Configuration hadoopConf) {
        return false;
    }
}
