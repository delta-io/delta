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

import io.delta.kernel.defaults.engine.fileio.PositionOutputStream;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.StreamCapabilities;

public class HadoopPositionOutputStream extends PositionOutputStream {
  private final FSDataOutputStream delegateStream;

  public HadoopPositionOutputStream(FSDataOutputStream delegateStream) {
    this.delegateStream = delegateStream;
  }

  @Override
  public void write(int b) throws IOException {
    delegateStream.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    delegateStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    delegateStream.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    delegateStream.flush();
  }

  @Override
  public void close() throws IOException {
    // If the write was aborted and the underlying stream supports a true abort (e.g. S3A cancels
    // its multipart upload), abort so nothing is published to the destination. Otherwise fall
    // back to a normal close; callers that stage via temp-then-rename skip the rename on abort.
    if (isAborted() && delegateStream.hasCapability(StreamCapabilities.ABORTABLE_STREAM)) {
      delegateStream.abort();
    } else {
      delegateStream.close();
    }
  }

  @Override
  public long getPos() throws IOException {
    return delegateStream.getPos();
  }
}
