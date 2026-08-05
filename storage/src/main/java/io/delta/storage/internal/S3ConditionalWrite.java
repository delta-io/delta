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

package io.delta.storage.internal;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Abortable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.s3a.AWSServiceIOException;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.RemoteFileChangedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.Options.CreateFileOptionKeys
    .FS_OPTION_CREATE_CONDITIONAL_OVERWRITE;

/**
 * Writes an S3 object with an atomic create-if-absent request.
 *
 * <p>The write ID stored in object metadata distinguishes a real conflict from a successful
 * request whose response was lost. This is required because S3A retries PUT and multipart
 * completion requests, and a retry of a conditional request can observe the object created by
 * its own first attempt.</p>
 */
public final class S3ConditionalWrite {

    private static final Logger LOG = LoggerFactory.getLogger(S3ConditionalWrite.class);

    static final String CONDITIONAL_CREATE_OPTION = FS_OPTION_CREATE_CONDITIONAL_OVERWRITE;
    static final String WRITE_ID_METADATA_KEY = "delta-log-store-write-id";
    static final String WRITE_ID_HEADER_OPTION =
        Constants.FS_S3A_CREATE_HEADER + "." + WRITE_ID_METADATA_KEY;
    static final String WRITE_ID_XATTR = Constants.XA_HEADER_PREFIX + WRITE_ID_METADATA_KEY;
    static final int REPLAY_BUFFER_MEMORY_LIMIT_BYTES = 1024 * 1024;

    private S3ConditionalWrite() {}

    /**
     * Writes all actions to {@code path}, appending one UTF-8 newline after each action.
     *
     * <p>All writes require an abortable output stream. Non-overwrite writes additionally require
     * atomic conditional create and owner metadata. A file system that cannot provide those
     * capabilities is rejected rather than downgraded to a process-local lock.</p>
     */
    public static void write(
            FileSystem fs,
            Path path,
            Iterator<String> actions,
            boolean overwrite) throws IOException {
        if (overwrite) {
            writeOverwrite(fs, path, actions);
            return;
        }

        writeConditional(fs, path, actions);
    }

    private static void writeOverwrite(
            FileSystem fs,
            Path path,
            Iterator<String> actions) throws IOException {
        final FSDataOutputStream stream = fs.create(path, true);
        requireAbortable(stream, path);
        try {
            writeActions(actions, stream, null);
        } catch (IOException | RuntimeException | Error failure) {
            abortAfterFailure(stream, failure);
            throw failure;
        }
        stream.close();
    }

    private static void writeConditional(
            FileSystem fs,
            Path path,
            Iterator<String> actions) throws IOException {
        final String writeId = UUID.randomUUID().toString();
        final FSDataOutputStream stream = createConditionalStream(fs, path, writeId);
        requireAbortable(stream, path);

        final S3WriteReplayBuffer replayBuffer =
            new S3WriteReplayBuffer(REPLAY_BUFFER_MEMORY_LIMIT_BYTES);
        Throwable primaryFailure = null;
        try {
            try {
                writeActions(actions, stream, replayBuffer);
                replayBuffer.seal();
            } catch (IOException | RuntimeException | Error failure) {
                abortAfterFailure(stream, failure);
                throw failure;
            }

            closeConditionalWrite(fs, path, writeId, stream, replayBuffer);
        } catch (IOException | RuntimeException | Error failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            try {
                replayBuffer.close();
            } catch (IOException | RuntimeException cleanupFailure) {
                if (primaryFailure == null) {
                    // A local cleanup error must not turn a committed S3 object into a reported
                    // failure. Failed immediate deletion also attempts JVM-exit cleanup.
                    LOG.warn("Failed to delete the S3 conditional-write replay buffer", cleanupFailure);
                } else {
                    primaryFailure.addSuppressed(cleanupFailure);
                }
            }
        }
    }

    private static FSDataOutputStream createConditionalStream(
            FileSystem fs,
            Path path,
            String writeId) throws IOException {
        final FSDataOutputStreamBuilder<?, ?> builder = fs.createFile(path);
        builder.overwrite(false);
        builder.must(CONDITIONAL_CREATE_OPTION, true);
        builder.must(WRITE_ID_HEADER_OPTION, writeId);
        // No action bytes have been submitted before build() returns, so a builder failure cannot
        // be reconciled as a successful write even if an eager file system created an object
        // carrying this write ID.
        return builder.build();
    }

    private static void writeActions(
            Iterator<String> actions,
            FSDataOutputStream stream,
            S3WriteReplayBuffer replayBuffer) throws IOException {
        while (actions.hasNext()) {
            final byte[] line = (actions.next() + "\n").getBytes(StandardCharsets.UTF_8);
            if (replayBuffer != null) {
                // Retain each line before giving it to S3. A local spill failure can then abort the
                // upload without leaving bytes that cannot be reproduced by a later retry.
                replayBuffer.write(line);
            }
            stream.write(line);
        }
    }

    private static void closeConditionalWrite(
            FileSystem fs,
            Path path,
            String writeId,
            FSDataOutputStream initialStream,
            S3WriteReplayBuffer replayBuffer) throws IOException {
        final int retryLimit = Math.max(
            0,
            fs.getConf().getInt(Constants.RETRY_LIMIT, Constants.RETRY_LIMIT_DEFAULT));
        FSDataOutputStream stream = initialStream;
        int retries = 0;

        while (true) {
            try {
                stream.close();
                return;
            } catch (IOException failure) {
                final ReconciliationResult reconciliation =
                    reconcileConditionalFailure(fs, path, writeId, failure);
                if (reconciliation == ReconciliationResult.OWN_WRITE_FOUND) {
                    return;
                }
                if (retries >= retryLimit) {
                    throw failure;
                }

                retries += 1;
                waitBeforeRetry(fs, path, retries, retryLimit, failure);
                try {
                    stream = createConditionalStream(fs, path, writeId);
                    requireAbortable(stream, path);
                    try {
                        replayBuffer.replayTo(stream);
                    } catch (IOException | RuntimeException | Error replayFailure) {
                        abortAfterFailure(stream, replayFailure);
                        throw replayFailure;
                    }
                } catch (IOException | RuntimeException | Error retryFailure) {
                    retryFailure.addSuppressed(failure);
                    throw retryFailure;
                }
            }
        }
    }

    private static void waitBeforeRetry(
            FileSystem fs,
            Path path,
            int retryNumber,
            int retryLimit,
            IOException failure) throws InterruptedIOException {
        final long intervalMillis = fs.getConf().getTimeDuration(
            Constants.RETRY_INTERVAL,
            Constants.RETRY_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);
        LOG.warn(
            "Retrying conditional S3 write to {} after HTTP 409 (retry {} of {})",
            path,
            retryNumber,
            retryLimit);
        if (intervalMillis <= 0) {
            return;
        }

        try {
            Thread.sleep(intervalMillis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            final InterruptedIOException interruptedWrite = new InterruptedIOException(
                "Interrupted while waiting to retry conditional S3 write to " + path);
            interruptedWrite.initCause(interrupted);
            interruptedWrite.addSuppressed(failure);
            throw interruptedWrite;
        }
    }

    private static void requireAbortable(FSDataOutputStream stream, Path path) {
        if (!stream.hasCapability(StreamCapabilities.ABORTABLE_STREAM)) {
            final UnsupportedOperationException failure = new UnsupportedOperationException(
                "The S3 output stream for " + path + " does not support abort()");
            abortAfterFailure(stream, failure);
            throw failure;
        }
    }

    private static void abortAfterFailure(FSDataOutputStream stream, Throwable failure) {
        try {
            final Abortable.AbortableResult result = stream.abort();
            if (result != null && result.anyCleanupException() != null) {
                failure.addSuppressed(result.anyCleanupException());
            }
        } catch (RuntimeException | Error abortFailure) {
            failure.addSuppressed(abortFailure);
        }
    }

    private static ReconciliationResult reconcileConditionalFailure(
            FileSystem fs,
            Path path,
            String writeId,
            IOException failure) throws IOException {
        final byte[] storedWriteId;
        try {
            storedWriteId = fs.getXAttr(path, WRITE_ID_XATTR);
        } catch (FileNotFoundException notFound) {
            failure.addSuppressed(notFound);
            if (isConditionalRequestConflict(failure)) {
                // S3 documents 409 as retryable. For multipart uploads, retrying requires a new
                // upload ID and re-uploading every part, so the caller replays the entire stream.
                return ReconciliationResult.RETRY_REQUIRED;
            }
            throw failure;
        } catch (IOException | UnsupportedOperationException reconciliationFailure) {
            failure.addSuppressed(reconciliationFailure);
            throw failure;
        }

        if (Arrays.equals(writeId.getBytes(StandardCharsets.UTF_8), storedWriteId)) {
            return ReconciliationResult.OWN_WRITE_FOUND;
        }

        if (isConditionalConflict(failure)) {
            final FileAlreadyExistsException conflict =
                new FileAlreadyExistsException(path.toString());
            conflict.initCause(failure);
            throw conflict;
        }

        // A non-conditional failure such as an authorization or transport error must retain its
        // original type even when HEAD happens to observe a foreign object at the destination.
        throw failure;
    }

    private static boolean isConditionalRequestConflict(IOException failure) {
        return failure instanceof AWSServiceIOException &&
            ((AWSServiceIOException) failure).statusCode() == 409;
    }

    private static boolean isConditionalConflict(IOException failure) {
        if (failure instanceof RemoteFileChangedException ||
                failure instanceof org.apache.hadoop.fs.FileAlreadyExistsException) {
            return true;
        }

        // S3 returns 409 when a conditional PUT or multipart completion races with another
        // operation. The caller reaches this check only after HEAD found a nonmatching owner, so
        // the otherwise ambiguous 409 now proves that another writer owns the destination.
        return isConditionalRequestConflict(failure);
    }

    private enum ReconciliationResult {
        OWN_WRITE_FOUND,
        RETRY_REQUIRED
    }
}
