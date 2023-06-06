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
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;

import java.util.Optional;

/**
 * Simple ExternalLogStore implementation using an in-memory hashmap (as opposed to an actual
 * database)
 */
public class MemoryLogStore extends BaseExternalLogStore {
    public MemoryLogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    @Override
    protected void putExternalEntry(
            ExternalCommitEntry entry,
            boolean overwrite) throws IOException {
        final String key = createKey(entry.tablePath.toString(), entry.fileName);
        final ExternalCommitEntry correctedEntry = new ExternalCommitEntry(
            // some tests use "failing:" scheme to inject errors, but we want to store normal paths
            new Path(fixPathSchema(entry.tablePath.toString())),
            entry.fileName,
            entry.tempPath,
            entry.complete,
            entry.expireTime
        );

        if (overwrite) {
            hashMap.put(key, correctedEntry);
        } else if (hashMap.containsKey(key)) { // and overwrite=false
            throw new java.nio.file.FileAlreadyExistsException("already exists");
        } else {
            hashMap.put(key, correctedEntry);
        }
    }

    @Override
    protected Optional<ExternalCommitEntry> getExternalEntry(
            String tablePath,
            String fileName) {
        final String key = createKey(tablePath, fileName);
        if (hashMap.containsKey(key)) {
            return Optional.of(hashMap.get(key));
        }
        return Optional.empty();
    }

    @Override
    protected Optional<ExternalCommitEntry> getLatestExternalEntry(Path tablePath) {
        final Path fixedTablePath = new Path(fixPathSchema(tablePath.toString()));
        return hashMap
            .values()
            .stream()
            .filter(item -> item.tablePath.equals(fixedTablePath))
            .max(Comparator.comparing(ExternalCommitEntry::absoluteFilePath));
    }

    /**
     * ExternalLogStoreSuite sometimes uses "failing:" scheme prefix to inject errors during tests
     * However, we want lookups for the same $tablePath to return the same result, regardless of
     * scheme.
     */
    static String fixPathSchema(String tablePath) {
        return tablePath.replace("failing:", "file:");
    }

    static String createKey(String tablePath, String fileName) {
        return String.format("%s-%s", fixPathSchema(tablePath), fileName);
    }

    static ExternalCommitEntry get(Path path) {
        final String tablePath = path.getParent().getParent().toString();
        final String fileName = path.getName();
        final String key = createKey(tablePath, fileName);
        return hashMap.get(key);
    }

    static boolean containsKey(Path path) {
        final String tablePath = path.getParent().getParent().toString();
        final String fileName = path.getName();
        final String key = createKey(tablePath, fileName);
        return hashMap.containsKey(key);
    }

    static ConcurrentHashMap<String, ExternalCommitEntry> hashMap = new ConcurrentHashMap<>();
}
