package io.delta.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;

import io.delta.storage.utils.ThrowingSupplier;
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

    public static final int DEFAULT_MAX_RETRIES = 3;

    private final ThrowingSupplier<CloseableIterator<String>, IOException> iterSupplier;

    private final int maxRetries;

    /**
     * Index of the last element successfully returned without an exception. A value of -1 means
     * that no element has ever been returned yet.
     */
    private int lastSuccessfullIndex;

    private int numRetries = 0;

    private CloseableIterator<String> currentIter;

    public RetryableCloseableIterator(
            ThrowingSupplier<CloseableIterator<String>, IOException> iterSupplier,
            int maxRetries) throws IOException {
        if (maxRetries < 0) throw new IllegalArgumentException("maxRetries can't be negative");

        this.iterSupplier = Objects.requireNonNull(iterSupplier);
        this.maxRetries = maxRetries;
        this.lastSuccessfullIndex = -1;
        this.currentIter = this.iterSupplier.get();
    }

    public RetryableCloseableIterator(
            ThrowingSupplier<CloseableIterator<String>, IOException> iterSupplier)
        throws IOException {

        this(iterSupplier, DEFAULT_MAX_RETRIES);
    }

    /////////////////
    // Public APIs //
    /////////////////

    @Override
    public void close() throws IOException {
        currentIter.close();
    }

    /**
     * `hasNext` must be idempotent. It does not change the `lastSuccessfulIndex` variable.
     */
    @Override
    public boolean hasNext() {
        try {
            return hasNextInternal();
        } catch (IOException ex) {
            if (isRemoteFileChangedException(ex)) {
                try {
                    replayIterToLastSuccessfulIndex(ex);
                } catch (IOException ex2) {
                    throw new UncheckedIOException(ex2);
                }
                return hasNext();
            } else {
                throw new UncheckedIOException(ex);
            }

        }
    }

    @Override
    public String next() {
        if (!hasNext()) throw new NoSuchElementException();

        try {
            final String ret = nextInternal();
            lastSuccessfullIndex++;
            return ret;
        } catch (IOException ex) {
            if (isRemoteFileChangedException(ex)) {
                try {
                    replayIterToLastSuccessfulIndex(ex);
                } catch (IOException ex2) {
                    throw new UncheckedIOException(ex2);
                }

                if (!hasNext()) {
                    throw new IllegalStateException(
                        String.format(
                            "A retried iterator doesn't have enough data " +
                                "(hasNext=false, lastSuccessfullIndex=%s)",
                            lastSuccessfullIndex
                        )
                    );
                }

                return next();
            } else {
                throw new UncheckedIOException(ex);
            }
        }
    }

    //////////////////////////////////////
    // Package-private APIs for testing //
    //////////////////////////////////////

    /** Visible for testing. */
    int getLastSuccessfullIndex() {
        return lastSuccessfullIndex;
    }

    /** Visible for testing. */
    int getNumRetries() {
        return numRetries;
    }

    ////////////////////
    // Helper Methods //
    ////////////////////

    /** Throw a checked exception so we can catch this in the caller. */
    private boolean hasNextInternal() throws IOException {
        return currentIter.hasNext();
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
            "Caught a RemoteFileChangedException. NumRetries is {} / {}.\n{}",
            numRetries + 1, maxRetries, topLevelEx
        );
        currentIter.close();

        while (numRetries < maxRetries) {
            numRetries++;
            LOG.info(
                "Replaying until (inclusive) index {}. NumRetries is {} / {}.",
                lastSuccessfullIndex, numRetries + 1, maxRetries
            );
            currentIter = iterSupplier.get();

            // Last successful index replayed. Starts at -1, and not 0, because 0 means we've
            // already replayed the 1st element!
            int replayIndex = -1;
            try {
                while (replayIndex < lastSuccessfullIndex) {
                    if (currentIter.hasNext()) {
                        currentIter.next(); // Disregard data that has been read
                        replayIndex++;
                    } else {
                        throw new IllegalStateException(
                            String.format(
                                "A retried iterator doesn't have enough data " +
                                    "(replayIndex=%s, lastSuccessfullIndex=%s)",
                                replayIndex,
                                lastSuccessfullIndex
                            )
                        );
                    }
                }

                // Just like how in RetryableCloseableIterator::next we have to handle
                // RemoteFileChangedException, we must also hadnle that here during the replay.
                // `currentIter.next()` isn't declared to throw a RemoteFileChangedException, so we
                // trick the compiler into thinking this block can throw RemoteFileChangedException
                // via `fakeIOException`. That way, we can catch it, and retry replaying the iter.
                fakeIOException();

                LOG.info("Successfully replayed until (inclusive) index {}", lastSuccessfullIndex);

                return;
            } catch (IOException ex) {
                if (isRemoteFileChangedException(ex)) {
                    // Ignore and try replaying the iter again at the top of the while loop
                    LOG.warn("Caught a RemoteFileChangedException while replaying the iterator");
                } else {
                    throw ex;
                }
            }
        }

        throw topLevelEx;
    }

    private boolean isRemoteFileChangedException(IOException ex) {
        // `endsWith` should still work if the class is shaded.
        final String exClassName = ex.getClass().getName();
        return exClassName.endsWith("org.apache.hadoop.fs.s3a.RemoteFileChangedException");
    }

    private void fakeIOException() throws IOException {
        if (false) {
            throw new IOException();
        }
    }
}
