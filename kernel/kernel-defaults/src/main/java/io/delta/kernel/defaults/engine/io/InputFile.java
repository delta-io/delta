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

import java.io.IOException;

/** Interface for reading a file and getting metadata about it. */
public interface InputFile {
  /**
   * Get the size of the file.
   *
   * @return the size of the file.
   */
  long length() throws IOException;

  /**
   * Get the path of the file.
   *
   * @return the path of the file.
   */
  String path();

  /**
   * Get the input stream to read the file.
   *
   * @return the input stream to read the file. It is the responsibility of the caller to close the
   *     stream.
   */
  SeekableInputStream newStream() throws IOException;
}
