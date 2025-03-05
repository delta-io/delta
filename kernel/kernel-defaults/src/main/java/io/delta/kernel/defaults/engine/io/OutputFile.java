/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.engine.io;

import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;

/** Interface for writing to a file and getting metadata about it. */
public interface OutputFile {
  /**
   * Get the path of the file.
   *
   * @return the path of the file.
   */
  String path();

  /**
   * Get the output stream to write to the file.
   *
   * <ul>
   *   <li>If the file already exists, (either at the time of creating the {@link
   *       PositionOutputStream} or at the time of closing it), it will be overwritten.
   *   <li>if {@code atomicWrite} is set, then the entire content is written or none, but won't
   *       create a file with the partial contents.
   * </ul>
   *
   * @return the output stream to write to the file. It is the responsibility of the caller to close
   *     the stream.
   * @throws IOException if an I/O error occurs.
   */
  PositionOutputStream create(boolean atomicWrite) throws IOException;

  /**
   * Atomically write (either write is completely or don't write all - i.e. don't leave file with
   * partial content) the data to a file at the given path. If the file already exists do not
   * replace it if {@code replace} is false. If {@code replace} is true, then replace the file with
   * the new data.
   *
   * <p>TODO: the semantics are very loose here, see if there is a better API name. One of the
   * reasons why the data is passed as an iterator is because of the existing LogStore interface
   * which are used in the Hadoop implementation of the {@link FileIO}
   *
   * @param data the data to write. Each element in the iterator is a line in the file.
   * @param overwrite if true, overwrite the file with the new data. If false, do not overwrite the
   *     file.
   * @throws java.nio.file.FileAlreadyExistsException if the file already exists and replace is
   *     false.
   * @throws IOException for any IO error.
   */
  void writeAtomically(CloseableIterator<String> data, boolean overwrite) throws IOException;
}
