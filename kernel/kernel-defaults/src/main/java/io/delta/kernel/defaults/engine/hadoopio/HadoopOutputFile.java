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

import static java.lang.String.format;

import io.delta.kernel.defaults.engine.fileio.OutputFile;
import io.delta.kernel.defaults.engine.fileio.PositionOutputStream;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.storage.LogStore;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopOutputFile implements OutputFile {
  private final Configuration hadoopConf;
  private final String path;

  public HadoopOutputFile(Configuration hadoopConf, String path) {
    this.hadoopConf = Objects.requireNonNull(hadoopConf, "fs is null");
    this.path = Objects.requireNonNull(path, "path is null");
  }

  @Override
  public String path() {
    return path;
  }

  @Override
  public PositionOutputStream create(boolean putIfAbsent) throws IOException {
    Path targetPath = new Path(path);
    FileSystem fs = targetPath.getFileSystem(hadoopConf);
    if (!putIfAbsent) {
      return new HadoopPositionOutputStream(fs.create(targetPath));
    }
    LogStore logStore =
        io.delta.kernel.defaults.internal.logstore.LogStoreProvider.getLogStore(
            hadoopConf, targetPath.toUri().getScheme());

    boolean useRename = logStore.isPartialWriteVisible(targetPath, hadoopConf);

    final Path writePath;
    if (useRename) {
      // In order to atomically write the file, write to a temp file and rename
      // to target path
      String tempFileName = format(".%s.%s.tmp", targetPath.getName(), UUID.randomUUID());
      writePath = new Path(targetPath.getParent(), tempFileName);
    } else {
      writePath = targetPath;
    }

    return new HadoopPositionOutputStream(fs.create(writePath)) {
      @Override
      public void close() throws IOException {
        super.close();
        if (useRename) {
          boolean renameDone = false;
          try {
            renameDone = fs.rename(writePath, targetPath);
            if (!renameDone) {
              if (fs.exists(targetPath)) {
                throw new java.nio.file.FileAlreadyExistsException(
                    "target file already exists: " + targetPath);
              }
              throw new IOException("Failed to rename the file");
            }
          } finally {
            if (!renameDone) {
              fs.delete(writePath, false /* recursive */);
            }
          }
        }
      }
    };
  }

  @Override
  public void writeAtomically(CloseableIterator<String> data, boolean overwrite)
      throws IOException {
    Path pathObj = new Path(path);
    try {
      LogStore logStore =
          io.delta.kernel.defaults.internal.logstore.LogStoreProvider.getLogStore(
              hadoopConf, pathObj.toUri().getScheme());
      logStore.write(pathObj, data, overwrite, hadoopConf);
    } finally {
      Utils.closeCloseables(data);
    }
  }
}
