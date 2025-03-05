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

import io.delta.kernel.defaults.engine.io.InputFile;
import io.delta.kernel.defaults.engine.io.OutputFile;
import io.delta.kernel.defaults.engine.io.PositionOutputStream;
import io.delta.kernel.defaults.engine.io.SeekableInputStream;
import java.io.IOException;
import org.apache.parquet.io.DelegatingPositionOutputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;

/**
 * Utilities related to Parquet I/O. These utilities bridge the gap between Kernel's {@link
 * io.delta.kernel.defaults.engine.io.FileIO} and the Parquet I/O classes.
 */
public class ParquetIOUtils {
  private ParquetIOUtils() {}

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
