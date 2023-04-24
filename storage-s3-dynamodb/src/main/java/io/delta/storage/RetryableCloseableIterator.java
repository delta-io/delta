package io.delta.storage;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;

import org.apache.hadoop.fs.s3a.RemoteFileChangedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class presents an iterator view over the iterator supplier in the constructor.
 *
 * This class assumes that the iterator supplied by the supplier can throw, and that subsequent
 * supplier.get() calls will return an iterator over the same data.
 *
 * If there are any RemoteFileChangedException during `next` and `hasNext` calls, will retry
 * at most `MAX_RETRIES` times.
 *
 * Internally, keeps track of the last-successfully-returned index. Upon retry, will iterate back
 * to that same position. If another RemoteFileChangedException occurs during that retry, will fail.
 * We can solve that exception inception later (iterators within iterators).
 */
public class RetryableCloseableIterator implements CloseableIterator<String> {
    private static final Logger LOG = LoggerFactory.getLogger(RetryableCloseableIterator.class);

    /** Visible for testing. */
    public static final int MAX_RETRIES = 3;

    private final Supplier<CloseableIterator<String>> iterSupplier;

    /**
     * Index of the last element successfully returned without an exception. A value of -1 means
     * that no element has ever been returned yet.
     */
    private int lastSuccessfullIndex;

    private int numRetries = 0;

    private CloseableIterator<String> currentIter;

    public RetryableCloseableIterator(Supplier<CloseableIterator<String>> iterSupplier) {
        this.iterSupplier = Objects.requireNonNull(iterSupplier);
        this.lastSuccessfullIndex = -1;
        this.currentIter = this.iterSupplier.get();
    }

    /** Visible for testing. */
    public int getLastSuccessfullIndex() {
        return lastSuccessfullIndex;
    }

    /** Visible for testing. */
    public int getNumRetries() {
        return numRetries;
    }

    @Override
    public void close() throws IOException {
        if (currentIter != null) {
            currentIter.close();
        }
    }

    /**
     * `hasNext` must be idempotent. It does not change the `lastSuccessfulIndex` variable.
     */
    @Override
    public boolean hasNext() {
        try {
            return hasNextInternal();
        } catch (RemoteFileChangedException ex) {
            LOG.warn(
                "Caught a RemoteFileChangedException in `hastNext`. NumRetries is {} / {}.\n{}",
                numRetries + 1, MAX_RETRIES, ex.toString()
            );
            if (numRetries < MAX_RETRIES) {
                numRetries++;
                replayIterToLastSuccessfulIndex();
                // Now, the currentImpl has been recreated and iterated to the same index
                return hasNext();
            } else {
                throw new RuntimeException(ex);
            }
        }
    }

    /** Throw a checked exception so we can catch this in the caller. */
    private boolean hasNextInternal() throws RemoteFileChangedException {
        return currentIter.hasNext();
    }

    @Override
    public String next() {
        if (!hasNext()) throw new NoSuchElementException();

        try {
            final String ret = nextInternal();
            lastSuccessfullIndex++;
            return ret;
        } catch (RemoteFileChangedException ex) {
            LOG.warn(
                "Caught a RemoteFileChangedException in `next`. NumRetries is {} / {}.\n{}",
                numRetries + 1, MAX_RETRIES, ex.toString()
            );
            if (numRetries < MAX_RETRIES) {
                numRetries++;
                replayIterToLastSuccessfulIndex();
                // Now, the currentImpl has been recreated and iterated to the same index
                return next();
            } else {
                throw new RuntimeException(ex);
            }
        }
    }

    /** Throw a checked exception so we can catch this in the caller. */
    private String nextInternal() throws RemoteFileChangedException {
        return currentIter.next();
    }

    /**
     * Called after a RemoteFileChangedException was thrown. Tries to replay the underlying
     * iter implementation (supplied by the `implSupplier`) to the last successful index, so that
     * the previous error open (hasNext, or next) can be retried.
     *
     * NOTE: This iter replay **itself** can throw a RemoteFileChangedException. Let's not deal
     *       with that for now - that would require handling exception inception.
     */
    private void replayIterToLastSuccessfulIndex() {
        // We still need to close the currentImpl, even though it threw
        try {
            currentIter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        LOG.info("Replaying until (inclusive) index {}", lastSuccessfullIndex);
        currentIter = iterSupplier.get(); // this last impl threw an exception and is useless!

        // Note: we iterate until `i` == `lastSuccessfullIndex`, so that index `i` is the last
        // successfully returned index.
        //
        // e.g. `i` starts at -1. after the 1st currentImpl.next() call, i will be incremented to 0.
        //      This makes sense as per the `lastSuccessfullIndex` semantics, since 0 is the last
        //      index to be successfully returned.
        // e.g. suppose `lastSuccessfulIndex` is 25. Then we have read 26 items, with indices 0 to
        //      25 inclusive. Then we want to iterate while i < lastSuccessfullIndex. After that,
        //      i will increment to 25 and we will exit the for loop.
        for (int i = -1; i < lastSuccessfullIndex; i++) {
            // Note: this does NOT touch RetryableCloseableIterator::next and so does not change
            //       the index
            currentIter.next();
        }

        LOG.info("Successfully replayed until (inclusive) index {}", lastSuccessfullIndex);
    }
}
