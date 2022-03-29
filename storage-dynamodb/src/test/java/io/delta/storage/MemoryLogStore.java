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
        final String key = createKey(entry.tablePath.toString(), entry.fileName);
        if (overwrite) {
            hashMap.put(key, entry);
        } else if (hashMap.containsKey(key)) { // and overwrite=false
            throw new java.nio.file.FileAlreadyExistsException("already exists");
        } else {
            hashMap.put(key, entry);
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
        return hashMap
            .values()
            .stream()
            .filter(item -> item.tablePath.equals(tablePath))
            .sorted((a, b) -> b.absoluteFilePath().compareTo(a.absoluteFilePath()))
            .findFirst();
    }

    static String createKey(String tablePath, String fileName) {
        return String.format("%s-%s", tablePath, fileName);
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
