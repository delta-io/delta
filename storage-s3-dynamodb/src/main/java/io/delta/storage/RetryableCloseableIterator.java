package io.delta.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
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
 * at most `MAX_RETRIES` times. If there are similar exceptions during the retry, those are handled
 * and count towards the MAX_RETRIES.
 *
 * Internally, keeps track of the last-successfully-returned index. Upon retry, will iterate back
 * to that same position.
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
        } catch (IOException ex) {
            // `endsWith` should still work if the class is shaded.
            final String exClassName = ex.getClass().getName();
            if (exClassName.endsWith("org.apache.hadoop.fs.s3a.RemoteFileChangedException")) {
                try {
                    replayIterToLastSuccessfulIndex(ex);
                } catch (IOException ex2) {
                    throw new RuntimeException(ex2);
                }
                return hasNext();
            } else {
                throw new UncheckedIOException(ex);
            }

        }
    }

    /** Throw a checked exception so we can catch this in the caller. */
    private boolean hasNextInternal() throws IOException {
        return currentIter.hasNext();
    }

    @Override
    public String next() {
        if (!hasNext()) throw new NoSuchElementException();

        try {
            final String ret = nextInternal();
            lastSuccessfullIndex++;
            return ret;
        } catch (IOException ex) {
            // `endsWith` should still work if the class is shaded.
            final String exClassName = ex.getClass().getName();
            if (exClassName.endsWith("org.apache.hadoop.fs.s3a.RemoteFileChangedException")) {
                try {
                    replayIterToLastSuccessfulIndex(ex);
                } catch (IOException ex2) {
                    throw new RuntimeException(ex2);
                }
                return next();
            } else {
                throw new UncheckedIOException(ex);
            }
        }
    }

    /** Throw a checked exception so we can catch this in the caller. */
    private String nextInternal() throws IOException {
        return currentIter.next();
    }

    /**
     * Called after a RemoteFileChangedException was thrown. Tries to replay the underlying
     * iter implementation (supplied by the `implSupplier`) to the last successful index, so that
     * the previous error open (hasNext, or next) can be retried. If a RemoteFileChangedException
     * is thrown while replaying the iter, we just increment the `numRetries` counter and try again.
     */
    private void replayIterToLastSuccessfulIndex(IOException topLevelEx) throws IOException {
        LOG.warn(
            "Caught a RemoteFileChangedException in `next`. NumRetries is {} / {}.\n{}",
            numRetries + 1, MAX_RETRIES, topLevelEx
        );
        currentIter.close();

        while (numRetries < MAX_RETRIES) {
            numRetries++;
            LOG.info("Replaying until (inclusive) index {}", lastSuccessfullIndex);
            currentIter = iterSupplier.get();
            int replayIndex = 0;
            try {
                while (replayIndex < lastSuccessfullIndex) {
                    if (currentIter.hasNext()) {
                        currentIter.next(); // Disregard data that has been read
                        replayIndex++;
                    } else {
                        throw new IllegalStateException(
                            "a retried iterator doesn't have enough data (currentIndex=" +
                                replayIndex + ", lastSuccessfullIndex=" + lastSuccessfullIndex + ")");
                    }
                }

                // Just like how in RetryableCloseableIterator::next we have to handle
                // RemoteFileChangedException, we must also hadnle that here during the replay.
                // `currentIter.next()` isn't declared to throw a RemoteFileChangedException, so we
                // trick the compiler into thinking this block can throw RemoteFileChangedException
                // via `fakeIOException`. That way, we can catch it, and retry replaying the iter.
                fakeIOException();

                return;
            } catch (IOException ex) {
                final String exClassName = ex.getClass().getName();
                if (exClassName.endsWith("org.apache.hadoop.fs.s3a.RemoteFileChangedException")) {
                    // Ignore and try replaying the iter again at the top of the while loop
                } else {
                    throw ex;
                }
            }
            LOG.info("Successfully replayed until (inclusive) index {}", lastSuccessfullIndex);
        }

        throw new IOException(topLevelEx);
    }

    private void fakeIOException() throws IOException {
        if (false) {
            throw new IOException();
        }
    }
}
