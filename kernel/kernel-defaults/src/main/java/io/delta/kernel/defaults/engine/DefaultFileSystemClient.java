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
package io.delta.kernel.defaults.engine;

import io.delta.kernel.defaults.internal.logstore.LogStoreProvider;
import io.delta.kernel.engine.FileReadRequest;
import io.delta.kernel.engine.FileSystemClient;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.storage.LogStore;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Default implementation of {@link FileSystemClient} based on Hadoop APIs. It takes a Hadoop {@link
 * Configuration} object to interact with the file system. The following optional configurations can
 * be set to customize the behavior of the client:
 *
 * <ul>
 *   <li>{@code io.delta.kernel.logStore.<scheme>.impl} - The class name of the custom {@link
 *       LogStore} implementation to use for operations on storage systems with the specified {@code
 *       scheme}. For example, to use a custom {@link LogStore} for S3 storage objects:
 *       <pre>{@code
 * <property>
 *   <name>io.delta.kernel.logStore.s3.impl</name>
 *   <value>com.example.S3LogStore</value>
 * </property>
 *
 * }</pre>
 *       If not set, the default LogStore implementation for the scheme will be used.
 *   <li>{@code delta.enableFastS3AListFrom} - Set to {@code true} to enable fast listing
 *       functionality when using a {@link LogStore} created for S3 storage objects.
 * </ul>
 *
 * The above list of options is not exhaustive. For a complete list of options, refer to the
 * specific implementation of {@link FileSystem}.
 */
public class DefaultFileSystemClient implements FileSystemClient {
  private final Configuration hadoopConf;

  /**
   * Create an instance of the default {@link FileSystemClient} implementation.
   *
   * @param hadoopConf Configuration to use. List of options to customize the behavior of the client
   *     can be found in the class documentation.
   */
  public DefaultFileSystemClient(Configuration hadoopConf) {
    this.hadoopConf = hadoopConf;
  }

  @Override
  public CloseableIterator<FileStatus> listFrom(String filePath) throws IOException {
    Path path = new Path(filePath);
    LogStore logStore = LogStoreProvider.getLogStore(hadoopConf, path.toUri().getScheme());

    return Utils.toCloseableIterator(logStore.listFrom(path, hadoopConf))
        .map(
            hadoopFileStatus ->
                FileStatus.of(
                    hadoopFileStatus.getPath().toString(),
                    hadoopFileStatus.getLen(),
                    hadoopFileStatus.getModificationTime()));
  }

  @Override
  public String resolvePath(String path) throws IOException {
    Path pathObject = new Path(path);
    FileSystem fs = pathObject.getFileSystem(hadoopConf);
    return fs.makeQualified(pathObject).toString();
  }

  @Override
  public CloseableIterator<ByteArrayInputStream> readFiles(
      CloseableIterator<FileReadRequest> readRequests) {
    return readRequests.map(
        elem -> getStream(elem.getPath(), elem.getStartOffset(), elem.getReadLength()));
  }

  @Override
  public boolean mkdirs(String path) throws IOException {
    Path pathObject = new Path(path);
    FileSystem fs = pathObject.getFileSystem(hadoopConf);
    return fs.mkdirs(pathObject);
  }

  @Override
  public boolean delete(String path) throws IOException {
    Path pathObject = new Path(path);
    FileSystem fs = pathObject.getFileSystem(hadoopConf);
    return fs.delete(pathObject, false);
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
        throw new RuntimeException(
            String.format(
                "IOException reading from file %s at offset %s size %s", filePath, offset, size),
            ex);
      }
    } catch (IOException ex) {
      throw new RuntimeException(
          String.format("Could not resolve the FileSystem for path %s", filePath), ex);
    }
  }
}
