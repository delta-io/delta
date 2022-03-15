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
import java.io.IOException;
import java.util.stream.Stream;
import java.util.concurrent.ConcurrentHashMap;

class FailingDynamoDBLogStore extends DynamoDBLogStore {

    private static java.util.Random rng = new java.util.Random();
    private ConcurrentHashMap<String, Float> errorRates;

    public FailingDynamoDBLogStore(Configuration hadoopConf) {
        super(hadoopConf);
        errorRates = new ConcurrentHashMap<>();
        String errorRatesDef = getParam(hadoopConf, "errorRates", "");
        for(String s: errorRatesDef.split(",")) {
            if(!s.contains("=")) continue;
            String[] parts = s.split("=", 2);
            if(parts.length == 2)
                errorRates.put(parts[0], Float.parseFloat(parts[1]));
        };
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
      float rate = errorRates.getOrDefault(name, 0.0f);
      if(rng.nextFloat() < rate) {
          throw new IOException(String.format("injected %s fail", name));
      }
    }
}
