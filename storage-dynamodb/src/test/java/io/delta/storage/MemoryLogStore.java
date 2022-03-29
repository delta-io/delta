package io.delta.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import java.util.Optional;

/**
 * TODO
 */
public class MemoryLogStore extends BaseExternalLogStore {
    public MemoryLogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    @Override
    protected void putExternalEntry(
            ExternalCommitEntry entry,
            boolean overwrite) throws IOException {
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
    protected Optional<ExternalCommitEntry> getExternalEntry(
            Path absoluteTablePath,
            Path absoluteJsonPath) {
        if (hashMap.containsKey(absoluteJsonPath)) {
            return Optional.of(hashMap.get(absoluteJsonPath));
        }

        return Optional.empty();
    }

    @Override
    protected Optional<ExternalCommitEntry> getLatestExternalEntry(Path tablePath) {
        return hashMap
            .values()
            .stream()
            .filter(item -> item.tablePath.equals(tablePath))
            .sorted((a, b) -> b.absoluteJsonPath().compareTo(a.absoluteJsonPath()))
            .findFirst();
    }

    static ConcurrentHashMap<Path, ExternalCommitEntry> hashMap = new ConcurrentHashMap<>();
}
