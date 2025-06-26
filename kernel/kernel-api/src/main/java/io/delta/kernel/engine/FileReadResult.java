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
package io.delta.kernel.engine;

import io.delta.kernel.data.ColumnarBatch;
import java.util.Objects;

/**
 * Represents the result of reading a batch of data in a file.
 *
 * <p>Encapsulates both the data read (as a {@link ColumnarBatch}) and the full path of the file
 * from which the data was read.
 */
public class FileReadResult {

  private final ColumnarBatch data;
  private final String filePath;

  /**
   * Constructs a {@code FileReadResult} object with the given data and file path.
   *
   * @param data the columnar batch of data read from the file
   * @param filePath the path of the file from which the data was read
   */
  public FileReadResult(ColumnarBatch data, String filePath) {
    this.data = Objects.requireNonNull(data, "data must not be null");
    this.filePath = Objects.requireNonNull(filePath, "filePath must not be null");
  }

  /** @return {@link ColumnarBatch} of data that was read from the file. */
  public ColumnarBatch getData() {
    return data;
  }

  /** @return the path of the file that this data was read from. */
  public String getFilePath() {
    return filePath;
  }
}
