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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.ByteArrayInputStream;
import java.io.IOException;

public class DelegateFileSystemClient implements FileSystemClient {

  private final FileSystemClient delegate;

  public DelegateFileSystemClient(FileSystemClient delegate) {
    this.delegate = requireNonNull(delegate);
  }

  @Override
  public CloseableIterator<FileStatus> listFrom(String filePath) throws IOException {
    return delegate.listFrom(filePath);
  }

  @Override
  public String resolvePath(String path) throws IOException {
    return delegate.resolvePath(path);
  }

  @Override
  public CloseableIterator<ByteArrayInputStream> readFiles(
      CloseableIterator<FileReadRequest> readRequests) throws IOException {
    return delegate.readFiles(readRequests);
  }

  @Override
  public boolean mkdirs(String path) throws IOException {
    return delegate.mkdirs(path);
  }

  @Override
  public boolean delete(String path) throws IOException {
    return delegate.delete(path);
  }
}
