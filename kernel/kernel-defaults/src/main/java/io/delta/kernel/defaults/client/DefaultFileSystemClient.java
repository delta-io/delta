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
package io.delta.kernel.defaults.client;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import io.delta.kernel.client.FileSystemClient;
import io.delta.kernel.client.FileReadRequest;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Utils;

/**
 * Default implementation of {@link FileSystemClient} based on Hadoop APIs.
 */
public class DefaultFileSystemClient
    implements FileSystemClient {
    private final Configuration hadoopConf;

    public DefaultFileSystemClient(Configuration hadoopConf) {
        this.hadoopConf = hadoopConf;
    }

    @Override
    public CloseableIterator<FileStatus> listFrom(String filePath) throws IOException {
        Iterator<org.apache.hadoop.fs.FileStatus> iter;

        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(hadoopConf);
        if (!fs.exists(path.getParent())) {
            throw new FileNotFoundException(
                String.format("No such file or directory: %s", path.getParent())
            );
        }
        org.apache.hadoop.fs.FileStatus[] files = fs.listStatus(path.getParent());
        iter = Arrays.stream(files)
            .filter(f -> f.getPath().getName().compareTo(path.getName()) >= 0)
            .sorted(Comparator.comparing(o -> o.getPath().getName()))
            .iterator();

        return Utils.toCloseableIterator(iter)
            .map(hadoopFileStatus ->
                FileStatus.of(
                    hadoopFileStatus.getPath().toString(),
                    hadoopFileStatus.getLen(),
                    hadoopFileStatus.getModificationTime())
            );
    }

    @Override
    public String resolvePath(String path) throws IOException {
        Path pathObject = new Path(path);
        FileSystem fs = pathObject.getFileSystem(hadoopConf);
        Path resolvedPath = fs.resolvePath(pathObject);
        return fs.makeQualified(resolvedPath).toString();
    }

    @Override
    public CloseableIterator<ByteArrayInputStream> readFiles(
        CloseableIterator<FileReadRequest> readRequests) {
        return readRequests.map(elem ->
                getStream(elem.getPath(), elem.getStartOffset(), elem.getReadLength()));
    }

    private ByteArrayInputStream getStream(String filePath, int offset, int size) {
        Path path = new Path(filePath);
        try {
            FileSystem fs = path.getFileSystem(hadoopConf);
            try (DataInputStream stream = fs.open(path)) {
                stream.skipBytes(offset);
                byte[] buff = new byte[size];
                stream.readFully(buff);
                return new ByteArrayInputStream(buff);
            } catch (IOException ex) {
                throw new RuntimeException(String.format(
                    "IOException reading from file %s at offset %s size %s",
                    filePath, offset, size), ex);
            }
        } catch (IOException ex) {
            throw new RuntimeException(String.format(
                "Could not resolve the FileSystem for path %s", filePath), ex);
        }
    }
}
