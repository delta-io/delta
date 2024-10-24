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

package io.delta.kernel.engine;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;

/**
 * Provides file system related functionalities to Delta Kernel. Delta Kernel uses this client
 * whenever it needs to access the underlying file system where the Delta table is present.
 * Connector implementation of this interface can hide filesystem specific details from Delta
 * Kernel.
 *
 * @since 3.0.0
 */
@Evolving
public interface FileSystemClient {
  /**
   * List the paths in the same directory that are lexicographically greater or equal to (UTF-8
   * sorting) the given `path`. The result should also be sorted by the file name.
   *
   * @param filePath Fully qualified path to a file
   * @return Closeable iterator of files. It is the responsibility of the caller to close the
   *     iterator.
   * @throws FileNotFoundException if the file at the given path is not found
   * @throws IOException for any other IO error.
   */
  CloseableIterator<FileStatus> listFrom(String filePath) throws IOException;

  /**
   * Resolve the given path to a fully qualified path.
   *
   * @param path Input path
   * @return Fully qualified path.
   * @throws FileNotFoundException If the given path doesn't exist.
   * @throws IOException for any other IO error.
   */
  String resolvePath(String path) throws IOException;

  /**
   * Return an iterator of byte streams one for each read request in {@code readRequests}. The
   * returned streams are in the same order as the given {@link FileReadRequest}s. It is the
   * responsibility of the caller to close each returned stream.
   *
   * @param readRequests Iterator of read requests
   * @return Data for each request as one {@link ByteArrayInputStream}.
   * @throws IOException
   */
  CloseableIterator<ByteArrayInputStream> readFiles(CloseableIterator<FileReadRequest> readRequests)
      throws IOException;

  /**
   * Create a directory at the given path including parent directories. This mimicks the behavior of
   * `mkdir -p` in Unix.
   *
   * @param path Full qualified path to create a directory at.
   * @return true if the directory was created successfully, false otherwise.
   * @throws IOException for any IO error.
   */
  boolean mkdirs(String path) throws IOException;

  /**
   * Delete the file at given path.
   *
   * @param path the path to delete. If path is a directory throws an exception.
   * @return true if delete is successful else false.
   * @throws IOException for any IO error.
   */
  boolean delete(String path) throws IOException;

  /**
   * Get the status of the file at the given path.
   *
   * @param path Fully qualified path of the file to check.
   * @return FileStatus object containing details such as file length, modification time, etc.
   * @throws FileNotFoundException If the file at the given path does not exist.
   * @throws IOException For any other IO error.
   */
  default FileStatus getFileStatus(String path) throws IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Check whether a file exists at the given path.
   *
   * @param path Fully qualified path of the file to check.
   * @return true if the file exists, false otherwise.
   * @throws IOException For any IO error while accessing the file system.
   */
  default boolean exists(String path) throws IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Copy a file from sourcePath to targetPath.
   *
   * <p>If {@code overwrite} is {@code true}, any existing file at the targetPath will be replaced
   * with the source file. If {@code overwrite} is {@code false} and a file already exists at the
   * targetPath, a {@link FileAlreadyExistsException} will be thrown.
   *
   * @param sourcePath Fully qualified path of the source file.
   * @param targetPath Fully qualified path of the target file.
   * @param overwrite If {@code true}, the target file is overwritten if it already exists. If
   *     {@code false}, an exception is thrown if the target file exists.
   * @throws FileAlreadyExistsException If a file already exists at the targetPath and {@code
   *     overwrite} is {@code false}.
   * @throws IOException For any other IO error while accessing the file system.
   */
  default void copy(String sourcePath, String targetPath, boolean overwrite) throws IOException {
    throw new UnsupportedOperationException("Not implemented");
  }
}
