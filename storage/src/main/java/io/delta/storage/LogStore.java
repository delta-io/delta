/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Iterator;

/**
 * :: DeveloperApi ::
 *
 * <p>
 * General interface for all critical file system operations required to read and write the
 * Delta logs. The correctness is predicated on the atomicity and durability guarantees of
 * the implementation of this interface. Specifically,
 * </p>
 * <ol>
 *     <li>Atomic visibility of files: If isPartialWriteVisible is false, any file written through
 *         this store must be made visible atomically. In other words, this should not generate
 *         partial files.</li>
 *
 *    <li>Mutual exclusion: Only one writer must be able to create (or rename) a file at the final
 *         destination.</li>
 *
 *    <li>Consistent listing: Once a file has been written in a directory, all future listings for
 *    that directory must return that file.</li>
 * </ol>
 * <p>
 * All subclasses of this interface is required to have a constructor that takes Configuration
 * as a single parameter. This constructor is used to dynamically create the LogStore.
 * </p>
 * <p>
 * LogStore and its implementations are not meant for direct access but for configuration based
 * on storage system. See [[https://docs.delta.io/latest/delta-storage.html]] for details.
 * </p>
 *
 * @since 1.0.0
 */
public abstract class LogStore {

  private Configuration initHadoopConf;

  public LogStore(Configuration initHadoopConf) {
    this.initHadoopConf = initHadoopConf;
  }

  /**
   * :: DeveloperApi ::
   *
   * Hadoop configuration that should only be used during initialization of LogStore. Each method
   * should use their `hadoopConf` parameter rather than this (potentially outdated) hadoop
   * configuration.
   */
  public Configuration initHadoopConf() { return initHadoopConf; }

  /**
   * :: DeveloperApi ::
   *
   * Load the given file and return an `Iterator` of lines, with line breaks removed from each line.
   * Callers of this function are responsible to close the iterator if they are done with it.
   *
   * @throws IOException if there's an issue resolving the FileSystem
   * @since 1.0.0
   */
  public abstract CloseableIterator<String> read(
        Path path,
        Configuration hadoopConf) throws IOException;

  /**
   * :: DeveloperApi ::
   *
   * Write the given `actions` to the given `path` with or without overwrite as indicated.
   * Implementation must throw {@link java.nio.file.FileAlreadyExistsException} exception if the
   * file already exists and overwrite = false. Furthermore, if isPartialWriteVisible returns false,
   * implementation must ensure that the entire file is made visible atomically, that is,
   * it should not generate partial files.
   *
   * @throws IOException if there's an issue resolving the FileSystem
   * @throws FileAlreadyExistsException if the file already exists and overwrite is false
   * @since 1.0.0
   */
  public abstract void write(
        Path path,
        Iterator<String> actions,
        Boolean overwrite,
        Configuration hadoopConf) throws IOException;

  /**
   * :: DeveloperApi ::
   *
   * List the paths in the same directory that are lexicographically greater or equal to
   * (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
   *
   * @throws IOException if there's an issue resolving the FileSystem
   * @throws FileAlreadyExistsException if {@code path} directory can't be found
   * @since 1.0.0
   */
  public abstract Iterator<FileStatus> listFrom(
        Path path,
        Configuration hadoopConf) throws IOException;

  /**
   * :: DeveloperApi ::
   *
   * List the entries of {@code path.getParent()} whose name is lexicographically greater than or
   * equal to (UTF-8 sorting) {@code path.getName()}, optionally descending into subdirectories.
   *
   * <ul>
   *   <li>Files and directories are both returned; directories have {@code isDirectory() == true}
   *       and are returned before anything under them.</li>
   *   <li>{@code path.getName()} only bounds the top level. Use {@code "\u0000"} to list
   *       everything.</li>
   *   <li>{@code filter} is applied to every entry's name, at every depth. A skipped directory is
   *       not returned and nothing under it is listed.</li>
   *   <li>With {@code recursive == false} the result equals {@link #listFrom(Path, Configuration)}
   *       minus the filtered entries.</li>
   *   <li>Throws {@link java.io.FileNotFoundException} if {@code path.getParent()} doesn't exist.
   *       A subdirectory that disappears while the listing runs is treated as empty.</li>
   * </ul>
   *
   * <p>The default implementation builds on {@link #listFrom(Path, Configuration)}, so every
   * existing LogStore supports it unchanged. Object-store implementations override it to push the
   * start offset, directory boundaries and skipped subtrees to the server.</p>
   *
   * @throws IOException if there's an issue resolving the FileSystem
   * @since 4.5.0
   */
  public Iterator<FileStatus> listFrom(
        Path path,
        boolean recursive,
        ListFilter filter,
        Configuration hadoopConf) throws IOException {
    return io.delta.storage.internal.RecursiveListing.listFrom(
        this, path, recursive, filter == null ? ListFilter.NONE : filter, hadoopConf);
  }

  /**
   * :: DeveloperApi ::
   *
   * Resolve the fully qualified path for the given `path`.
   *
   * @throws IOException if there's an issue resolving the FileSystem
   * @since 1.0.0
   */
  public abstract Path resolvePathOnPhysicalStorage(
        Path path,
        Configuration hadoopConf) throws IOException;

  /**
   * :: DeveloperApi ::
   *
   * Whether a partial write is visible for the underlying file system of `path`.
   *
   * @throws IOException if there's an issue resolving the FileSystem
   * @since 1.0.0
   */
  public abstract Boolean isPartialWriteVisible(
        Path path,
        Configuration hadoopConf) throws IOException;
}
