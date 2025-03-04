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

import io.delta.kernel.defaults.engine.io.InputFile;
import io.delta.kernel.defaults.engine.io.SeekableInputStream;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopInputFile implements InputFile {
  private final FileSystem fs;
  private final Path path;

  public HadoopInputFile(FileSystem fs, Path path) {
    this.fs = fs;
    this.path = path;
  }

  @Override
  public long length() throws IOException {
    return fs.getFileStatus(path).getLen();
  }

  @Override
  public String path() {
    return path.toString();
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    return new HadoopSeekableInputStream(fs.open(path));
  }
}
