/*
 * Copyright (2026) The Delta Lake Project Authors.
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
 * LogStore implementation for GooseFS ({@code gfs://}).
 * <p>
 * This class does not depend on the GooseFS client. Callers put
 * {@code com.qcloud.cos.goosefs.hadoop.FileSystem} on the classpath and register it for the
 * {@code gfs} scheme. The Hadoop client returns {@code false} from {@code rename} when the
 * destination file already exists, instead of throwing
 * {@link java.nio.file.FileAlreadyExistsException}. {@link #writeWithRename} maps that
 * {@code false} back to {@link java.nio.file.FileAlreadyExistsException} so optimistic
 * transactions can retry.
 * <p>
 * Each commit is a new file: the actions are written with {@code create}, closed, then renamed
 * into place in the same directory. This store does not append to an existing file.
 * {@code isPartialWriteVisible} is {@code true} because a GooseFS file becomes list-visible
 * before {@code completeFile} returns.
 */
public class GooseFSLogStore extends HadoopFileSystemLogStore {

    public GooseFSLogStore(Configuration hadoopConf) {
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
