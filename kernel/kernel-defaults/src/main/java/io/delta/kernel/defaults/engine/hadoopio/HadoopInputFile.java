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

import io.delta.kernel.defaults.engine.fileio.InputFile;
import io.delta.kernel.defaults.engine.fileio.SeekableInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Options.OpenFileOptions;
import org.apache.hadoop.fs.Path;

public class HadoopInputFile implements InputFile {
  private final FileSystem fs;
  private final Path path;
  private final long fileSize;

  public HadoopInputFile(FileSystem fs, Path path, long fileSize) {
    this.fs = fs;
    this.path = path;
    this.fileSize = fileSize;
  }

  @Override
  public long length() throws IOException {
    return fileSize;
  }

  @Override
  public String path() {
    return path.toString();
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    // Pass the known length so the file system can skip the HEAD call to fetch the file size;
    // omit it when unknown.
    FutureDataInputStreamBuilder builder = fs.openFile(path);
    if (fileSize > 0) {
      builder.opt(OpenFileOptions.FS_OPTION_OPENFILE_LENGTH, Long.toString(fileSize));
    }
    try {
      FSDataInputStream in = builder.build().get();
      return new HadoopSeekableInputStream(in);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedIOException("Interrupted while opening " + path);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException("Failed to open " + path, cause);
    }
  }
}
