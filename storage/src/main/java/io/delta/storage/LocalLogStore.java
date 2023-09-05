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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

import java.io.IOException;
import java.util.Iterator;

/**
 * Default {@link LogStore} implementation (should be used for testing only!).
 *
 * Production users should specify the appropriate {@link LogStore} implementation in Spark properties.<p>
 *
 * We assume the following from {@link FileSystem} implementations:
 * <ul>
 *  <li>Rename without overwrite is atomic.</li>
 *  <li>List-after-write is consistent.</li>
 * </ul>
 * Regarding file creation, this implementation:
 * <ul>
 *  <li>Uses atomic rename when overwrite is false; if the destination file exists or the rename
 *   fails, throws an exception. </li>
 *  <li>Uses create-with-overwrite when overwrite is true. This does not make the file atomically
 *   visible and therefore the caller must handle partial files.</li>
 * </ul>
 */
public class LocalLogStore extends HadoopFileSystemLogStore{
    public LocalLogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    /**
     * This write implementation needs to wrap `writeWithRename` with `synchronized` as rename()
     * for {@link RawLocalFileSystem} doesn't throw an exception when the target file
     * exists. Hence, we must make sure `exists + rename` in `writeWithRename` is atomic in our tests.
     */
    @Override
    public void write(
            Path path,
            Iterator<String> actions,
            Boolean overwrite,
            Configuration hadoopConf) throws IOException {
        synchronized(this) {
            writeWithRename(path, actions, overwrite, hadoopConf);
        }
    }

    @Override
    public Boolean isPartialWriteVisible(Path path, Configuration hadoopConf) throws IOException {
        return true;
    }
}
