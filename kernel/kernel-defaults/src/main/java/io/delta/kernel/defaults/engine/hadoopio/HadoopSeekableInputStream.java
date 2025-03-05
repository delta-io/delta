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
package io.delta.kernel.defaults.engine.hadoopio;

import io.delta.kernel.defaults.engine.fileio.SeekableInputStream;
import java.util.Objects;
import org.apache.hadoop.fs.FSDataInputStream;

public class HadoopSeekableInputStream extends SeekableInputStream {
  private final FSDataInputStream delegateStream;

  public HadoopSeekableInputStream(FSDataInputStream delegateStream) {
    this.delegateStream = Objects.requireNonNull(delegateStream, "delegateStream is null");
  }

  @Override
  public int read() throws java.io.IOException {
    return delegateStream.read();
  }

  @Override
  public int read(byte[] b) throws java.io.IOException {
    return delegateStream.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws java.io.IOException {
    return delegateStream.read(b, off, len);
  }

  @Override
  public long skip(long n) throws java.io.IOException {
    return delegateStream.skip(n);
  }

  @Override
  public int available() throws java.io.IOException {
    return delegateStream.available();
  }

  @Override
  public void close() throws java.io.IOException {
    delegateStream.close();
  }

  @Override
  public void seek(long pos) throws java.io.IOException {
    delegateStream.seek(pos);
  }

  @Override
  public long getPos() throws java.io.IOException {
    return delegateStream.getPos();
  }
}
