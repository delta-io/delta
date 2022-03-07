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
import java.util.Iterator;

public abstract class BaseExternalLogStore extends HadoopFileSystemLogStore {

    ////////////////////////
    // Public API Methods //
    ////////////////////////

    public BaseExternalLogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    @Override
    public Iterator<FileStatus> listFrom(Path path, Configuration hadoopConf) throws IOException {
        // TODO
        return null;
    }

    @Override
    public void write(
            Path path,
            Iterator<String> actions,
            Boolean overwrite,
            Configuration hadoopConf) throws IOException {
        // TODO
    }

    @Override
    public Boolean isPartialWriteVisible(Path path, Configuration hadoopConf) throws IOException {
        return false;
    }

    /////////////////////////////////////////////////////////////
    // Protected Members (for interaction with external store) //
    /////////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////
    // Protected Members (for error injection during tests) //
    //////////////////////////////////////////////////////////

    ////////////////////
    // Helper Methods //
    ////////////////////
}
