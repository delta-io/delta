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
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * LogStore implementation for Azure.
 * <p>
 * We assume the following from Azure's [[FileSystem]] implementations:
 * - Rename without overwrite is atomic.
 * - List-after-write is consistent.
 * <p>
 * Regarding file creation, this implementation:
 * - Uses atomic rename when overwrite is false; if the destination file exists or the rename
 * fails, throws an exception.
 * - Uses create-with-overwrite when overwrite is true. This does not make the file atomically
 * visible and therefore the caller must handle partial files.
 */

public class AzureLogStore extends HadoopFileSystemLogStore {
    private static final Logger LOG = LoggerFactory.getLogger(AzureLogStore.class);

    public AzureLogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    @Override
    public void write(Path path,
                      Iterator<String> actions,
                      Boolean overwrite,
                      Configuration hadoopConf) throws IOException {
        writeWithRename(path, actions, overwrite, hadoopConf);
    }

    public Boolean isPartialWriteVisible(Path path, Configuration hadoopConf) {
        return true;
    }
}
