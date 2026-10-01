/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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
 * Opt-in LogStore implementation that uses S3 conditional writes for mutual exclusion across
 * drivers.
 *
 * <p>This implementation requires a Hadoop S3A filesystem that supports mandatory conditional
 * create options, custom create headers, extended-attribute reads, and abortable output streams.
 * Unsupported capabilities fail the write instead of falling back to JVM-local coordination.</p>
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
            return;
        }

        final FileSystem fs = path.getFileSystem(hadoopConf);
        final Path resolvedPath = BaseS3LogStore.resolvePathWithoutUserInfo(fs, path);
        S3ConditionalWrite.write(fs, resolvedPath, actions);
    }
}
