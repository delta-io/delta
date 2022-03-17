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

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * LogStore implementation for Azure.
 * <p>
 * We assume the following from Azure's [[FileSystem]] implementations:
 * <ul>
 *     <li>Rename without overwrite is atomic.</li>
 *     <li>List-after-write is consistent.</li>
 * </ul>
 * <p>
 * Regarding file creation, this implementation:
 *  <ul>
 *     <li>Uses atomic rename when overwrite is false; if the destination file exists or the rename
 *         fails, throws an exception.</li>
 *     <li>Uses create-with-overwrite when overwrite is true. This does not make the file atomically
 *         visible and therefore the caller must handle partial files.</li>
 * </ul>
 */
public class AzureLogStore extends HadoopFileSystemLogStore {

    public AzureLogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    @Override
    public void write(
            Path path,
            Iterator<String> actions,
            Boolean overwrite,
            Configuration hadoopConf) throws IOException {
        writeWithRename(path, actions, overwrite, hadoopConf);
    }

    @Override
    public Boolean isPartialWriteVisible(Path path, Configuration hadoopConf) {
        return true;
    }
}
