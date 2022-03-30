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
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.storage.internal.LogStoreUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelegatingLogStore extends LogStore {

    //////////////////////////////////////
    // Static Helper Fields and Methods //
    //////////////////////////////////////

    private static final Logger LOG = LoggerFactory.getLogger(DelegatingLogStore.class);

    public static final String DEFAULT_S3_LOG_STORE_CLASS_NAME =
        S3SingleDriverLogStore.class.getName();

    public static final String DEFAULT_AZURE_LOG_STORE_CLASS_NAME =
        AzureLogStore.class.getName();

    public static final String DEFAULT_HDFS_LOG_STORE_CLASS_NAME = HDFSLogStore.class.getName();

    public static final Set<String> S3_SCHEMES =
            new HashSet<>(Arrays.asList("s3", "s3a", "s3n"));

    public static final Set<String> AZURE_SCHEMES =
            new HashSet<>(Arrays.asList("abfs", "abfss", "adl", "wasb", "wasbs"));

    public static final Set<String> ALL_SCHEMES =
        Stream.concat(S3_SCHEMES.stream(), AZURE_SCHEMES.stream()).collect(Collectors.toSet());

    public static String getDefaultLogStoreClassName(String scheme) {
        if (S3_SCHEMES.contains(scheme)) {
            return DEFAULT_S3_LOG_STORE_CLASS_NAME;
        } else if (AZURE_SCHEMES.contains(scheme)) {
            return DEFAULT_AZURE_LOG_STORE_CLASS_NAME;
        }
        return DEFAULT_HDFS_LOG_STORE_CLASS_NAME;
    }

    //////////////////////////////////
    // Constructor & Helper Methods //
    //////////////////////////////////

    // Map scheme to the corresponding LogStore resolved and created. Accesses to this map needs
    // synchronization. This could be accessed by multiple threads because it is shared through
    // shared DeltaLog instances.
    private final Map<String, LogStore> schemaToLogStoreMap;

    public DelegatingLogStore(Configuration initHadoopConf) {
        super(initHadoopConf);

        this.schemaToLogStoreMap = new HashMap<>();
    }

    public LogStore getDelegateByScheme(Path path, Configuration hadoopConf) throws IOException {
        final String origScheme = path.toUri().getScheme();

        synchronized (this) {
            final String scheme = origScheme == null ? null : origScheme.toLowerCase(Locale.ROOT);
            if (schemaToLogStoreMap.containsKey(scheme)) {
                return schemaToLogStoreMap.get(scheme);
            } else {
                final String className;

                if (scheme == null) {
                    className = DEFAULT_HDFS_LOG_STORE_CLASS_NAME;
                } else {
                    // Resolve LogStore class based on the following order:
                    // 1. Scheme conf if set.
                    // 2. Defaults for scheme if exists.
                    // 3. HDFS log store (implicitly handled by `getDefaultLogStoreClassName`).
                    className = hadoopConf.get(
                            LogStoreUtils.logStoreSchemeConfKey(scheme),
                            getDefaultLogStoreClassName(scheme));
                }

                final LogStore logStore;
                try {
                     logStore = LogStoreUtils.createLogStoreWithClassName(className, hadoopConf);
                } catch (ClassNotFoundException | NoSuchMethodException |
                        InvocationTargetException | InstantiationException |
                        IllegalAccessException e) {
                    // This method is used by all DelegatingLogStore public APIs, which each throw
                    // IOException. It's not worth changing the parent LogStore interface and
                    // exposing this specific implementation Exception types, so we catch and wrap
                    // those exceptions, instead.
                    throw new IOException("DelegatingLogStore: internal error", e);
                }

                schemaToLogStoreMap.put(scheme, logStore);

                LOG.info(
                    String.format(
                        "LogStore %s is used for scheme %s", logStore.getClass().getName(), scheme
                    )
                );

                return logStore;
            }
        }
    }

    //////////////////////////
    // Public API Overrides //
    //////////////////////////

    @Override
    public CloseableIterator<String> read(Path path, Configuration hadoopConf) throws IOException {
        return getDelegateByScheme(path, hadoopConf).read(path, hadoopConf);
    }

    @Override
    public void write(
            Path path,
            Iterator<String> actions,
            Boolean overwrite,
            Configuration hadoopConf) throws IOException {
        getDelegateByScheme(path, hadoopConf).write(path, actions, overwrite, hadoopConf);
    }

    @Override
    public Iterator<FileStatus> listFrom(Path path, Configuration hadoopConf) throws IOException {
        return getDelegateByScheme(path, hadoopConf).listFrom(path, hadoopConf);
    }

    @Override
    public Path resolvePathOnPhysicalStorage(
            Path path,
            Configuration hadoopConf) throws IOException {
        return getDelegateByScheme(path, hadoopConf).resolvePathOnPhysicalStorage(path, hadoopConf);
    }

    @Override
    public Boolean isPartialWriteVisible(Path path, Configuration hadoopConf) throws IOException {
        return getDelegateByScheme(path, hadoopConf).isPartialWriteVisible(path, hadoopConf);
    }
}
