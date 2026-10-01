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
 * Single JVM LogStore implementation for S3.
 * <p>
 * We assume the following from S3's {@link FileSystem} implementations:
 * <ul>
 *   <li>File writing on S3 is all-or-nothing, whether overwrite or not.</li>
 *   <li>List-after-write is strongly consistent.</li>
 * </ul>
 * <p>
 * Regarding file creation, this implementation:
 * <ul>
 *   <li>Opens a stream to write to S3 (regardless of the overwrite option).</li>
 *   <li>Failures during stream write may leak resources, but may never result in partial
 *       writes.</li>
 * </ul>
 */
public class S3SingleDriverLogStore extends BaseS3LogStore {

    public S3SingleDriverLogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    @Override
    public void write(
            Path path,
            Iterator<String> actions,
            Boolean overwrite,
            Configuration hadoopConf) throws IOException {
        writeWithPathLock(path, actions, overwrite, hadoopConf);
    }
}
