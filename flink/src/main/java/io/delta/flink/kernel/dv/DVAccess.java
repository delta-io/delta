/*
 *  Copyright (2026) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.kernel.dv;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import java.lang.reflect.Field;
import java.util.zip.CRC32;

/** Base for deletion-vector file-format implementations. */
public abstract class DVAccess {

  public abstract void write(Engine engine, String filePath, RoaringBitmapArray bitmap);

  public abstract RoaringBitmapArray read(Engine engine, String filePath);

  /**
   * Write {@code bitmap} to a freshly-named DV file under {@code tableRootPath} and return a {@link
   * DeletionVectorDescriptor} suitable for attaching to an {@code AddFile}. Encapsulates everything
   * format-specific -- file naming, in-file offset, base85 encoding -- so callers don't have to
   * track those details when the DV layout changes.
   *
   * @param engine Kernel engine providing file-system access.
   * @param tableRootPath the table data path (e.g. {@code file:///...}); the DV file is placed
   *     directly under this path with an implementation-chosen name.
   * @param bitmap deletion vector to write.
   * @return a descriptor whose {@link DeletionVectorDescriptor#getAbsolutePath(String) absolute
   *     path} (passed the same {@code tableRootPath}) resolves to the file just written.
   */
  public abstract DeletionVectorDescriptor store(
      Engine engine, String tableRootPath, RoaringBitmapArray bitmap);

  /** Reflective handle on the private {@code DefaultEngine.fileIO}; resolved once at class load. */
  private static final Field DEFAULT_ENGINE_FILE_IO_FIELD;

  static {
    try {
      DEFAULT_ENGINE_FILE_IO_FIELD = DefaultEngine.class.getDeclaredField("fileIO");
      DEFAULT_ENGINE_FILE_IO_FIELD.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new ExceptionInInitializerError(
          "DefaultEngine.fileIO field not found; Kernel API drift?");
    }
  }

  protected static FileIO fileIOFromEngine(Engine engine) {
    if (!(engine instanceof DefaultEngine)) {
      throw new IllegalArgumentException(
          "DVAccess currently requires a DefaultEngine instance; got "
              + (engine == null ? "null" : engine.getClass().getName()));
    }
    try {
      return (FileIO) DEFAULT_ENGINE_FILE_IO_FIELD.get(engine);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Failed to read DefaultEngine.fileIO via reflection", e);
    }
  }

  protected static int crc32(byte[] data) {
    CRC32 crc = new CRC32();
    crc.update(data);
    return (int) crc.getValue();
  }
}
