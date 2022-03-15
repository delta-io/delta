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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class DelegatingLogStore extends LogStore {

    //////////////////////////////////////
    // Static Helper Fields and Methods //
    //////////////////////////////////////

    public static final String DEFAULT_S3_LOG_STORE_CLASS_NAME = null;
            // classOf[S3SingleDriverLogStore].getName();

    public static final String DEFAULT_AZURE_LOG_STORE_CLASS_NAME = null;
            // classOf[AzureLogStore].getName();

    public static final String DEFAULT_HDFS_LOG_STORE_CLASS_NAME = HDFSLogStore.class.getName();

    public static final Set<String> S3_SCHEMES =
            new HashSet<>(Arrays.asList("s3", "s3a", "s3n"));

    public static final Set<String> AZURE_SCHEMES =
            new HashSet<>(Arrays.asList("abfs", "abfss", "adl", "wasb", "wasbs"));

    public static String getDefaultLogStoreClassName(String scheme) {
        if (S3_SCHEMES.contains(scheme)) {
            return DEFAULT_S3_LOG_STORE_CLASS_NAME;
        } else if (AZURE_SCHEMES.contains(scheme)) {
            return DEFAULT_AZURE_LOG_STORE_CLASS_NAME;
        }
        return DEFAULT_HDFS_LOG_STORE_CLASS_NAME;
    }

    /////////////////
    // Public APIs //
    /////////////////

    private LogStore defaultLogStore = null;

    public DelegatingLogStore(Configuration initHadoopConf) {
        super(initHadoopConf);
    }
}
