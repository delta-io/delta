/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.internal.parquet;

import io.delta.kernel.defaults.engine.fileio.InputFile;
import io.delta.kernel.defaults.engine.fileio.OutputFile;
import io.delta.kernel.defaults.engine.fileio.PositionOutputStream;
import io.delta.kernel.defaults.engine.fileio.SeekableInputStream;
import io.delta.kernel.defaults.engine.hadoopio.HadoopInputFile;
import java.io.IOException;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.DelegatingPositionOutputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;

/**
 * Utilities related to Parquet I/O. These utilities bridge the gap between Kernel's {@link
 * io.delta.kernel.defaults.engine.fileio.FileIO} and the Parquet I/O classes.
 */
public class ParquetIOUtils {
  private ParquetIOUtils() {}

  /**
   * Returns the {@link ParquetConfiguration} to read {@code inputFile} with.
   *
   * <p>This matters for concurrency, not just for honoring settings. `parquet-mr` entry points
   * taking an {@link org.apache.parquet.io.InputFile} that isn't {@code
   * org.apache.parquet.hadoop.util.HadoopInputFile} fall back to {@code new
   * HadoopParquetConfiguration()}, which wraps a fresh {@code Configuration(loadDefaults=true)}.
   * Hadoop loads such a configuration's properties lazily, so the first property read -- which
   * `parquet-mr` issues immediately while building the read options -- scans the classpath for
   * `core-default.xml` and friends under a JVM-global lock. This would make concurrent readers
   * serialize on the lock.
   *
   * <p>Reusing the configuration the file is already being read with avoids the scan entirely: its
   * properties are loaded once and memoized on the instance. A non-{@code HadoopInputFile} input
   * carries no Hadoop configuration to reuse, so it falls back to a fresh {@code
   * HadoopParquetConfiguration}.
   */
  static ParquetConfiguration parquetConfiguration(InputFile inputFile) {
    return inputFile instanceof HadoopInputFile
        ? new HadoopParquetConfiguration(((HadoopInputFile) inputFile).configuration())
        : new HadoopParquetConfiguration();
  }

  /**
   * Reads the footer of {@code parquetFile} using {@code conf}, so that `parquet-mr` doesn't
   * construct a fresh Hadoop Configuration for the read. See {@link #parquetConfiguration}.
   */
  static ParquetMetadata readFooter(
      org.apache.parquet.io.InputFile parquetFile, ParquetConfiguration conf) throws IOException {
    ParquetReadOptions readOptions =
        ParquetReadOptions.builder(conf)
            .withMetadataFilter(ParquetMetadataConverter.NO_FILTER)
            .build();
    try (org.apache.parquet.io.SeekableInputStream stream = parquetFile.newStream()) {
      return ParquetFileReader.readFooter(parquetFile, readOptions, stream);
    }
  }

  /** Create a Parquet {@link org.apache.parquet.io.InputFile} from a Kernel's {@link InputFile}. */
  static org.apache.parquet.io.InputFile createParquetInputFile(InputFile inputFile) {
    return new org.apache.parquet.io.InputFile() {
      @Override
      public long getLength() throws IOException {
        return inputFile.length();
      }

      @Override
      public org.apache.parquet.io.SeekableInputStream newStream() throws IOException {
        SeekableInputStream seekableStream = inputFile.newStream();
        return new DelegatingSeekableInputStream(seekableStream) {
          @Override
          public void seek(long newPos) throws IOException {
            seekableStream.seek(newPos);
          }

          @Override
          public long getPos() throws IOException {
            return seekableStream.getPos();
          }
        };
      }
    };
  }

  /**
   * Create a Parquet {@link org.apache.parquet.io.OutputFile} from a Kernel's {@link OutputFile}.
   */
  static org.apache.parquet.io.OutputFile createParquetOutputFile(
      OutputFile kernelOutputFile, boolean atomicWrite) {
    return new org.apache.parquet.io.OutputFile() {
      @Override
      public org.apache.parquet.io.PositionOutputStream create(long blockSizeHint)
          throws IOException {
        // blockSizeHint is hint used in HDFS compliant file systems. In cloud storage systems
        // it is irrelevant. So, we ignore it.
        PositionOutputStream posOutputStream = kernelOutputFile.create(atomicWrite);
        return new DelegatingPositionOutputStream(posOutputStream) {
          @Override
          public long getPos() throws IOException {
            return posOutputStream.getPos();
          }
        };
      }

      @Override
      public org.apache.parquet.io.PositionOutputStream createOrOverwrite(long blockSizeHint)
          throws IOException {
        // In Kernel we never overwrite files, so this method is not used.
        throw new UnsupportedOperationException("createOrOverwrite is not supported in Kernel");
      }

      @Override
      public boolean supportsBlockSize() {
        return false;
      }

      @Override
      public long defaultBlockSize() {
        // blockSizeHint is hint used in HDFS compliant file systems. In cloud storage systems
        // it is irrelevant. So, return some default value.
        return 128 * 1024 * 1024; // 128MB
      }

      @Override
      public String getPath() {
        return kernelOutputFile.path();
      }
    };
  }
}
