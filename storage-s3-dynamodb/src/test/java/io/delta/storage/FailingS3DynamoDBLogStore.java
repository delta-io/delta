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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * An ExternalLogStore implementation that allows for easy, probability-based error injection during
 * runtime.
 *
 * This is used to test the error-handling capabilities of S3DynamoDBLogStore during integration
 * tests.
 */
public class FailingS3DynamoDBLogStore extends S3DynamoDBLogStore {

    private static java.util.Random rng = new java.util.Random();
    private final ConcurrentHashMap<String, Float> errorRates;

    public FailingS3DynamoDBLogStore(Configuration hadoopConf) throws IOException {
        super(hadoopConf);
        errorRates = new ConcurrentHashMap<>();

        // for each optional key in set { write_copy_temp_file, write_put_db_entry,
        // fix_delta_log_copy_temp_file, fix_delta_log_put_db_entry }, `errorRates` string is
        // expected to be of form key1=value1,key2=value2 etc where each value is a fraction
        // indicating how often that method should fail (e.g. 0.10 ==> 10% failure rate).
        String errorRatesDef = getParam(hadoopConf, "errorRates", "");
        for (String s: errorRatesDef.split(",")) {
            if (!s.contains("=")) continue;
            String[] parts = s.split("=", 2);
            if (parts.length == 2) {
                errorRates.put(parts[0], Float.parseFloat(parts[1]));
            }
        }
    }

    @Override
    protected void writeCopyTempFile(FileSystem fs, Path src, Path dst) throws IOException {
        injectError("write_copy_temp_file");
        super.writeCopyTempFile(fs, src, dst);
    }

    @Override
    protected void writePutCompleteDbEntry(ExternalCommitEntry entry) throws IOException {
        injectError("write_put_db_entry");
        super.writePutCompleteDbEntry(entry);
    }

    @Override
    protected void fixDeltaLogCopyTempFile(FileSystem fs, Path src, Path dst) throws IOException {
        injectError("fix_delta_log_copy_temp_file");
        super.fixDeltaLogCopyTempFile(fs, src, dst);
    }

    @Override
    protected void fixDeltaLogPutCompleteDbEntry(ExternalCommitEntry entry) throws IOException {
        injectError("fix_delta_log_put_db_entry");
        super.fixDeltaLogPutCompleteDbEntry(entry);
    }

    private void injectError(String name) throws IOException {
      float rate = errorRates.getOrDefault(name, 0.1f);
      if (rng.nextFloat() < rate) {
          throw new IOException(String.format("injected failure: %s", name));
      }
    }
}
