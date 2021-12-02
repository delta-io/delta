/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone.storage;

import java.io.FileNotFoundException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import io.delta.standalone.data.CloseableIterator;

/**
 * :: DeveloperApi ::
 * <p>
 * General interface for all critical file system operations required to read and write the
 * Delta logs. The correctness is predicated on the atomicity and durability guarantees of
 * the implementation of this interface. Specifically,
 * <ol>
 *     <li>
 *         Atomic visibility of files: If isPartialWriteVisible is false, any file written through
 *         this store must be made visible atomically. In other words, this should not generate
 *         partial files.
 *     </li>
 *     <li>
 *         Mutual exclusion: Only one writer must be able to create (or rename) a file at the final
 *         destination.
 *     </li>
 *     <li>
 *         Consistent listing: Once a file has been written in a directory, all future listings for
 *         that directory must return that file.
 *     </li>
 * </ol>
 * <p>
 * All subclasses of this interface are required to have a constructor that takes
 * {@link Configuration} as a single parameter. This constructor is used to dynamically create the
 * LogStore.
 * <p>
 * LogStore and its implementations are not meant for direct access but for configuration based
 * on storage system.
 *
 * @see <a href="https://docs.delta.io/latest/delta-storage.html" target="_blank">Delta Storage</a>
 * @since 0.3.0
 */
public abstract class LogStore {

    private Configuration initHadoopConf;

    public LogStore(Configuration initHadoopConf) {
        this.initHadoopConf = initHadoopConf;
    }

    /**
     * :: DeveloperApi ::
     * <p>
     * Hadoop configuration that should only be used during initialization of LogStore. Each method
     * should use their {@code hadoopConf} parameter rather than this (potentially outdated) hadoop
     * configuration.
     *
     * @return the initial hadoop configuration.
     */
    public Configuration initHadoopConf() { return initHadoopConf; }

    /**
     * :: DeveloperApi ::
     * <p>
     * Load the given file and return an {@link Iterator} of lines, with line breaks removed from
     * each line. Callers of this function are responsible to close the iterator if they are done
     * with it.
     *
     * @since 0.3.0
     *
     * @param path  the path to load
     * @param hadoopConf  the latest hadoopConf
     * @return the CloseableIterator of lines in the given file.
     */
    public abstract CloseableIterator<String> read(Path path, Configuration hadoopConf);

    /**
     * :: DeveloperApi ::
     * <p>
     * Write the given actions to the given {@link Path} with or without overwrite as indicated.
     * <p>
     * Implementation must throw {@link java.nio.file.FileAlreadyExistsException} exception if the
     * file already exists and overwrite = {@code false}. Furthermore, if
     * {@link #isPartialWriteVisible} returns {@code false}, implementation must ensure that the
     * entire file is made visible atomically, that is, it should not generate partial files.
     *
     * @since 0.3.0
     *
     * @param path  the path to write to
     * @param actions  actions to be written
     * @param overwrite  if true, overwrites the file if it already exists
     * @param hadoopConf  the latest hadoopConf
     * @throws FileAlreadyExistsException if the file already exists and {@code overwrite} is
     *                                    {@code false}
     */
    public abstract void write(
        Path path,
        Iterator<String> actions,
        Boolean overwrite,
        Configuration hadoopConf) throws FileAlreadyExistsException;

    /**
     * :: DeveloperApi ::
     * <p>
     * List the paths in the same directory that are lexicographically greater or equal to
     * (UTF-8 sorting) the given {@link Path}. The result should also be sorted by the file name.
     *
     * @since 0.3.0
     *
     * @param path  the path to load
     * @param hadoopConf  the latest hadoopConf
     * @return an Iterator of the paths lexicographically greater or equal to (UTF-8 sorting) the
     *         given {@link Path}
     * @throws FileNotFoundException if the file does not exist
     */
    public abstract Iterator<FileStatus> listFrom(
        Path path,
        Configuration hadoopConf) throws FileNotFoundException;

    /**
     * :: DeveloperApi ::
     * <p>
     * Resolve the fully qualified path for the given {@link Path}.
     *
     * @since 0.3.0
     *
     * @param path  the path to resolve
     * @param hadoopConf  the latest hadoopConf
     * @return the resolved path
     */
    public abstract Path resolvePathOnPhysicalStorage(Path path, Configuration hadoopConf);

    /**
     * :: DeveloperApi ::
     * <p>
     * Whether a partial write is visible for the underlying file system of the given {@link Path}.
     *
     * @since 0.3.0
     *
     * @param path  the path in question
     * @param hadoopConf  the latest hadoopConf
     * @return true if partial writes are visible for the given {@link Path}, else false
     */
    public abstract Boolean isPartialWriteVisible(Path path, Configuration hadoopConf);
}
