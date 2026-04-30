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

package io.delta.storage.unitycatalog.hadoop.fs;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;

/**
 * Hadoop {@link FileSystem} wrapper that lets several UC credential scopes coexist in one Spark
 * session without forcing one global filesystem cache entry per cloud scheme.
 */
public class CredScopedFileSystem extends FilterFileSystem {

  private static final String CRED_SCOPED_FS_CACHE_MAX_SIZE =
      "unitycatalog.credScopedFs.cache.maxSize";
  private static final long CRED_SCOPED_FS_CACHE_MAX_SIZE_DEFAULT = 100;

  static final Map<CredScopedKey, FileSystem> CACHE;

  static {
    final long maxSize =
        Long.getLong(CRED_SCOPED_FS_CACHE_MAX_SIZE, CRED_SCOPED_FS_CACHE_MAX_SIZE_DEFAULT);
    CACHE =
        Collections.synchronizedMap(
            new LinkedHashMap<CredScopedKey, FileSystem>(16, 0.75f, true) {
              @Override
              protected boolean removeEldestEntry(Map.Entry<CredScopedKey, FileSystem> eldest) {
                if (size() <= maxSize) {
                  return false;
                }
                closeQuietly(eldest.getValue());
                return true;
              }
            });
  }

  static void clearCacheForTesting() {
    synchronized (CACHE) {
      CACHE.values().forEach(CredScopedFileSystem::closeQuietly);
      CACHE.clear();
    }
  }

  FileSystem getDelegate() {
    return this.fs;
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    CredScopedKey key = CredScopedKey.create(uri, conf);
    synchronized (CACHE) {
      FileSystem cachedFs = CACHE.get(key);
      if (cachedFs == null) {
        cachedFs = newFileSystem(uri, conf);
        CACHE.put(key, cachedFs);
      }
      this.fs = cachedFs;
    }
  }

  private static void restoreImpl(Configuration fsConf, String key, String defaultImpl) {
    fsConf.set(key, fsConf.get(key + ".original", defaultImpl));
  }

  private static FileSystem newFileSystem(URI uri, Configuration conf) throws IOException {
    Configuration fsConf = new Configuration(conf);

    restoreImpl(fsConf, "fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    restoreImpl(fsConf, "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    restoreImpl(fsConf, "fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3A");
    restoreImpl(fsConf, "fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A");
    fsConf.set("fs.s3.impl.disable.cache", "true");
    fsConf.set("fs.s3a.impl.disable.cache", "true");

    restoreImpl(fsConf, "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    restoreImpl(
        fsConf, "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    fsConf.set("fs.gs.impl.disable.cache", "true");

    restoreImpl(fsConf, "fs.abfs.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem");
    restoreImpl(fsConf, "fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem");
    restoreImpl(fsConf, "fs.AbstractFileSystem.abfs.impl", "org.apache.hadoop.fs.azurebfs.Abfs");
    restoreImpl(fsConf, "fs.AbstractFileSystem.abfss.impl", "org.apache.hadoop.fs.azurebfs.Abfss");
    fsConf.set("fs.abfs.impl.disable.cache", "true");
    fsConf.set("fs.abfss.impl.disable.cache", "true");

    return FileSystem.get(uri, fsConf);
  }

  private static void closeQuietly(FileSystem fs) {
    if (fs == null) {
      return;
    }
    try {
      fs.close();
    } catch (IOException e) {
      // ignore close failures on eviction
    }
  }
}
