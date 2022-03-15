package io.delta.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Collection;

import java.util.function.Predicate;
import java.util.Optional;

public class MemoryLogStore extends BaseExternalLogStore {
    public MemoryLogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    @Override
    protected void putExternalEntry(
        ExternalCommitEntry entry, boolean overwrite
    ) throws IOException {
        if (overwrite) {
            hashMap.put(entry.absoluteJsonPath(), entry);
        } else {
            ExternalCommitEntry curr_val = hashMap.putIfAbsent(entry.absoluteJsonPath(), entry);
            if (curr_val != null) {
                throw new java.nio.file.FileAlreadyExistsException("already exists");
            }
        }
    }

    @Override
    protected ExternalCommitEntry getExternalEntry(
        Path absoluteTablePath, Path absoluteJsonPath
    ) throws IOException {
        return get(absoluteJsonPath);
    }

    @Override
    protected ExternalCommitEntry getLatestExternalEntry(Path tablePath) throws IOException {
        return hashMap
            .values()
            .stream()
            .filter(item -> item.tablePath.equals(tablePath))
            .sorted((a, b) -> b.absoluteJsonPath().compareTo(a.absoluteJsonPath()))
            .findFirst().orElse(null);
    }

    static ConcurrentHashMap<Path, ExternalCommitEntry> hashMap = new ConcurrentHashMap<>();

    public static boolean containsKey(Path path) {
        return hashMap.containsKey(path);
    }

    public static ExternalCommitEntry get(Path path) {
        return hashMap.get(path);
    }
}
