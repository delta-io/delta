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

import io.delta.storage.internal.S3ConditionalWrite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Opt-in S3 LogStore using native conditional publication for mutual exclusion across drivers.
 *
 * <p>Requires Hadoop S3A 3.4.2 or later, conditional creation, user metadata and abortable,
 * immediately visible output streams. Unsupported configurations fail the write. All writers
 * must use a compatible atomic publication protocol and preserve immutable destination keys
 * during recovery. Mixing this store with unconditional writers is unsafe.</p>
 *
 * <p>A lost acknowledgement is recovered when the destination carries this invocation's UUID.
 * Inconclusive recovery throws an IOException with an unknown outcome: the write may have
 * committed. Neither whole-upload replay nor process-restart recovery is provided.</p>
 */
public class S3LogStore extends BaseS3LogStore {

    public S3LogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    @Override
    public void write(
            Path path,
            Iterator<String> actions,
            Boolean overwrite,
            Configuration hadoopConf) throws IOException {
        if (overwrite) {
            writeWithPathLock(path, actions, true, hadoopConf);
        } else {
            final FileSystem fs = path.getFileSystem(hadoopConf);
            S3ConditionalWrite.write(fs, resolvePathWithoutUserInfo(fs, path), actions);
        }
    }
}
