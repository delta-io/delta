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
package io.delta.kernel.internal.checkpoints;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.FileReadRequest;
import io.delta.kernel.engine.FileSystemClient;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/** Reads the `_last_checkpoint` hint file as a single UTF-8 JSON blob. */
final class LastCheckpointFileReader {
  private LastCheckpointFileReader() {}

  /**
   * Reads the entire `_last_checkpoint` file in one bounded {@link FileSystemClient#readFiles}
   * request. The file size comes from {@link FileSystemClient#getFileStatus(String)} so the read is
   * not split across multiple requests (which could observe a torn write).
   */
  static Optional<String> readUtf8(Engine engine, String path) throws IOException {
    FileSystemClient fileSystemClient = engine.getFileSystemClient();
    final FileStatus status = fileSystemClient.getFileStatus(path);

    if (status.getSize() == 0) {
      return Optional.empty();
    }

    checkArgument(
        status.getSize() <= Integer.MAX_VALUE,
        "_last_checkpoint file is too large: %s bytes at %s",
        status.getSize(),
        path);

    FileReadRequest readRequest =
        new FileReadRequest() {
          @Override
          public String getPath() {
            return path;
          }

          @Override
          public int getStartOffset() {
            return 0;
          }

          @Override
          public int getReadLength() {
            return (int) status.getSize();
          }
        };

    try (CloseableIterator<ByteArrayInputStream> streams =
        fileSystemClient.readFiles(singletonCloseableIterator(readRequest))) {
      if (!streams.hasNext()) {
        return Optional.empty();
      }
      try (InputStream in = streams.next()) {
        byte[] bytes = in.readAllBytes();
        String json = new String(bytes, StandardCharsets.UTF_8).trim();
        return json.isEmpty() ? Optional.empty() : Optional.of(json);
      }
    }
  }
}
