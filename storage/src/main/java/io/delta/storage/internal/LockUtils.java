package io.delta.storage.internal;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.Path;

public final class LockUtils {

    private LockUtils() { } // should not be instantiated

    /**
     * Release the lock for the path after writing.
     *
     * Note: the caller should resolve the path to make sure we are locking the correct absolute
     * path.
     */
    public static void releasePathLock(
            ConcurrentHashMap<Path, Object> pathLock,
            Path resolvedPath) {
        final Object lock = pathLock.remove(resolvedPath);
        synchronized(lock) {
            lock.notifyAll();
        }
    }

    /**
     * Acquire a lock for the path before writing.
     *
     * Note: the caller should resolve the path to make sure we are locking the correct absolute
     * path.
     */
    public static void acquirePathLock(
            ConcurrentHashMap<Path, Object> pathLock,
            Path resolvedPath) throws InterruptedException {
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
