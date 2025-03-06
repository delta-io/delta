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

import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.defaults.engine.fileio.InputFile;
import io.delta.kernel.defaults.engine.fileio.OutputFile;
import io.delta.kernel.defaults.engine.fileio.SeekableInputStream;
import io.delta.kernel.defaults.internal.logstore.LogStoreProvider;
import io.delta.kernel.engine.FileReadRequest;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.storage.LogStore;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Implementation of {@link FileIO} based on Hadoop APIs. */
public class HadoopFileIO implements FileIO {
  private final Configuration hadoopConf;

  public HadoopFileIO(Configuration hadoopConf) {
    this.hadoopConf = Objects.requireNonNull(hadoopConf, "hadoopConf is null");
  }

  @Override
  public CloseableIterator<FileStatus> listFrom(String filePath) throws IOException {
    Path path = new Path(filePath);
    LogStore logStore = LogStoreProvider.getLogStore(hadoopConf, path.toUri().getScheme());

    return Utils.toCloseableIterator(logStore.listFrom(path, hadoopConf))
        .map(
            hadoopFileStatus ->
                FileStatus.of(
                    hadoopFileStatus.getPath().toString(),
                    hadoopFileStatus.getLen(),
                    hadoopFileStatus.getModificationTime()));
  }

  @Override
  public FileStatus getFileStatus(String path) throws IOException {
    Path pathObject = new Path(path);
    FileSystem fs = pathObject.getFileSystem(hadoopConf);
    org.apache.hadoop.fs.FileStatus hadoopFileStatus = fs.getFileStatus(pathObject);
    return FileStatus.of(
        hadoopFileStatus.getPath().toString(),
        hadoopFileStatus.getLen(),
        hadoopFileStatus.getModificationTime());
  }

  @Override
  public String resolvePath(String path) throws IOException {
    Path pathObject = new Path(path);
    FileSystem fs = pathObject.getFileSystem(hadoopConf);
    return fs.makeQualified(pathObject).toString();
  }

  @Override
  public CloseableIterator<ByteArrayInputStream> readFiles(
      CloseableIterator<FileReadRequest> readRequests) throws IOException {
    return readRequests.map(
        elem -> getStream(elem.getPath(), elem.getStartOffset(), elem.getReadLength()));
  }

  @Override
  public boolean mkdirs(String path) throws IOException {
    Path pathObject = new Path(path);
    FileSystem fs = pathObject.getFileSystem(hadoopConf);
    return fs.mkdirs(pathObject);
  }

  @Override
  public InputFile newInputFile(String path) {
    return new HadoopInputFile(getFs(path), new Path(path));
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new HadoopOutputFile(hadoopConf, path);
  }

  @Override
  public boolean delete(String path) throws IOException {
    FileSystem fs = getFs(path);
    return fs.delete(new Path(path), false);
  }

  @Override
  public Optional<String> getConf(String confKey) {
    return Optional.ofNullable(hadoopConf.get(confKey));
  }

  private ByteArrayInputStream getStream(String filePath, int offset, int size) {
    Path path = new Path(filePath);
    try {
      InputFile inputFile = new HadoopInputFile(path.getFileSystem(hadoopConf), path);
      try (SeekableInputStream stream = inputFile.newStream()) {
        stream.seek(offset);
        byte[] buff = new byte[size];
        stream.readFully(buff, 0, size);
        return new ByteArrayInputStream(buff);
      } catch (IOException ex) {
        throw new UncheckedIOException(
            String.format(
                "IOException reading from file %s at offset %s size %s", filePath, offset, size),
            ex);
      }
    } catch (IOException ex) {
      throw new UncheckedIOException(
          String.format("Could not resolve the FileSystem for path %s", filePath), ex);
    }
  }

  private FileSystem getFs(String path) {
    try {
      Path pathObject = new Path(path);
      return pathObject.getFileSystem(hadoopConf);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not resolve the FileSystem", e);
    }
  }
}
