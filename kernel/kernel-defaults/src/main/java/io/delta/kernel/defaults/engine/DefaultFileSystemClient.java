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

import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.defaults.engine.fileio.InputFile;
import io.delta.kernel.defaults.engine.fileio.SeekableInputStream;
import io.delta.kernel.engine.FileReadRequest;
import io.delta.kernel.engine.FileSystemClient;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.storage.LogStore;
import java.io.*;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

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
  private final FileIO fileIO;

  /**
   * Create an instance of the default {@link FileSystemClient} implementation.
   *
   * @param fileIO The {@link FileIO} implementation to use for file operations.
   */
  public DefaultFileSystemClient(FileIO fileIO) {
    this.fileIO = Objects.requireNonNull(fileIO, "fileIO is null");
  }

  @Override
  public CloseableIterator<FileStatus> listFrom(String filePath) throws IOException {
    return fileIO.listFrom(filePath);
  }

  @Override
  public String resolvePath(String path) throws IOException {
    return fileIO.resolvePath(path);
  }

  @Override
  public CloseableIterator<ByteArrayInputStream> readFiles(
      CloseableIterator<FileReadRequest> readRequests) throws IOException {
    return readRequests.map(
        elem -> getStream(elem.getPath(), elem.getStartOffset(), elem.getReadLength()));
  }

  @Override
  public boolean mkdirs(String path) throws IOException {
    return fileIO.mkdirs(path);
  }

  @Override
  public boolean delete(String path) throws IOException {
    return fileIO.delete(path);
  }

  private ByteArrayInputStream getStream(String filePath, int offset, int size) {
    InputFile inputFile = this.fileIO.newInputFile(filePath, /* fileSize */ -1);
    try (SeekableInputStream stream = inputFile.newStream()) {
      stream.seek(offset);
      byte[] buff = new byte[size];
      stream.readFully(buff, 0, size);
      return new ByteArrayInputStream(buff);
    } catch (IOException ex) {
      throw new UncheckedIOException(
          String.format(
              "IOException reading from file %s at offset %s size %s", filePath, offset, size),
          ex);
    }
  }
}
