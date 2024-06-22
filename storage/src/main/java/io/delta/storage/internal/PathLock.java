package io.delta.storage.internal;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.Path;

/**
 * A lock that provides per-file-path `acquire` and `release` semantics. Can be used to ensure that
 * no two writers are creating the same external (e.g. S3) file at the same time.
 * <p>
 * Note: For all APIs, the caller should resolve the path to make sure we are locking the correct
 * absolute path.
 */
public class PathLock {

    private final ConcurrentHashMap<Path, Object> pathLock;

    public PathLock() {
        this.pathLock = new ConcurrentHashMap<>();
    }

    /** Release the lock for the path after writing. */
    public void release(Path resolvedPath) {
        final Object lock = pathLock.remove(resolvedPath);
        synchronized(lock) {
            lock.notifyAll();
        }
    }

    /** Acquire a lock for the path before writing. */
    public void acquire(Path resolvedPath) throws InterruptedException {
        while (true) {
            final Object lock = pathLock.putIfAbsent(resolvedPath, new Object());
            if (lock == null) {
                return;
            }
            synchronized (lock) {
                while (pathLock.get(resolvedPath) == lock) {
                    lock.wait();
                }
            }
        }
    }
}
