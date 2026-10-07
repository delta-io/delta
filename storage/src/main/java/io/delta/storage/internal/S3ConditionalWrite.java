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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;

import org.apache.hadoop.fs.Abortable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.s3a.AWSServiceIOException;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.RemoteFileChangedException;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;

import static org.apache.hadoop.fs.Options.CreateFileOptionKeys
    .FS_OPTION_CREATE_CONDITIONAL_OVERWRITE;

/** A single conditional publication, with ownership recovery but no whole-upload replay. */
public final class S3ConditionalWrite {
    private static final String WRITE_ID = "delta-log-store-write-id";
    private static final String WRITE_ID_OPTION = Constants.FS_S3A_CREATE_HEADER + "." + WRITE_ID;
    private static final String WRITE_ID_XATTR = Constants.XA_HEADER_PREFIX + WRITE_ID;

    private S3ConditionalWrite() {}

    public static void write(FileSystem fs, Path path, Iterator<String> actions) throws IOException {
        if (!(fs instanceof S3AFileSystem)
                || !fs.hasPathCapability(path, FS_OPTION_CREATE_CONDITIONAL_OVERWRITE)
                || !fs.hasPathCapability(path, Constants.FS_S3A_CREATE_HEADER)) {
            throw new UnsupportedOperationException(
                "Native S3 LogStore requires S3A conditional creation and user metadata: " + path);
        }

        final String writeId = UUID.randomUUID().toString();
        final FSDataOutputStream stream;
        try {
            final FSDataOutputStreamBuilder<?, ?> builder = fs.createFile(path);
            builder.overwrite(false);
            builder.must(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE, true);
            builder.must(WRITE_ID_OPTION, writeId);
            stream = builder.build();
        } catch (org.apache.hadoop.fs.FileAlreadyExistsException failure) {
            throw conflict(path, failure);
        }

        try {
            requirePublicationCapabilities(stream, path);
            while (actions.hasNext()) {
                stream.write((actions.next() + "\n").getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException | RuntimeException | Error failure) {
            // close() publishes. Never invoke it after failing to consume the complete payload.
            abort(stream, failure);
            throw failure;
        }

        try {
            stream.close();
        } catch (IOException failure) {
            abort(stream, failure);
            reconcile(fs, path, writeId, failure);
        } catch (RuntimeException | Error failure) {
            abort(stream, failure);
            throw failure;
        }
    }

    private static void requirePublicationCapabilities(FSDataOutputStream stream, Path path) {
        if (!stream.hasCapability(StreamCapabilities.ABORTABLE_STREAM)
                || !stream.hasCapability(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE)
                || stream.hasCapability(CommitConstants.STREAM_CAPABILITY_MAGIC_OUTPUT)) {
            throw new UnsupportedOperationException(
                "Native S3 LogStore requires an abortable conditional stream with immediate "
                    + "publication on close (magic streams are unsupported): " + path);
        }
    }

    private static void reconcile(
            FileSystem fs, Path path, String writeId, IOException failure) throws IOException {
        if (Thread.currentThread().isInterrupted() || isInterruption(failure)) {
            throw unknown(path, failure, null, true);
        }

        final byte[] owner;
        try {
            // getXAttr falls back to key + "/" after a 404. Require a file first so that a
            // directory marker cannot turn an unresolved failure into an OCC conflict.
            // This two-request check relies on the immutable-key requirement of this store.
            if (!fs.getFileStatus(path).isFile()) {
                throw new IOException("Conditional destination is not a file: " + path);
            }
            owner = fs.getXAttr(path, WRITE_ID_XATTR);
        } catch (IOException | UnsupportedOperationException probeFailure) {
            throw unknown(path, failure, probeFailure,
                Thread.currentThread().isInterrupted() || isInterruption(probeFailure));
        }
        if (Thread.currentThread().isInterrupted()) {
            throw unknown(path, failure, null, true);
        }

        if (Arrays.equals(writeId.getBytes(StandardCharsets.UTF_8), owner)) {
            return;
        }
        if (isConditionalConflict(failure)) {
            throw conflict(path, failure);
        }
        throw unknown(path, failure, null, false);
    }

    private static boolean isConditionalConflict(IOException failure) {
        // Inspect the publication exception, never message text or suppressed cleanup failures.
        if (failure instanceof RemoteFileChangedException
                || failure instanceof org.apache.hadoop.fs.FileAlreadyExistsException) {
            return true;
        }
        if (failure instanceof AWSServiceIOException) {
            final int status = ((AWSServiceIOException) failure).statusCode();
            return status == 409 || status == 412;
        }
        return false;
    }

    private static FileAlreadyExistsException conflict(Path path, IOException failure) {
        final FileAlreadyExistsException conflict = new FileAlreadyExistsException(path.toString());
        conflict.initCause(failure);
        return conflict;
    }

    private static IOException unknown(
            Path path, IOException failure, Throwable probeFailure, boolean interrupted) {
        final String message = "Unknown outcome of conditional S3 write to " + path
            + "; the object may have been committed. Do not blindly retry the transaction.";
        final IOException result;
        if (interrupted) {
            Thread.currentThread().interrupt();
            result = new InterruptedIOException(message);
        } else {
            result = new IOException(message);
        }
        result.initCause(failure);
        if (probeFailure != null && probeFailure != failure) {
            result.addSuppressed(probeFailure);
        }
        return result;
    }

    private static boolean isInterruption(Throwable failure) {
        // SocketTimeoutException is an InterruptedIOException, but does not cancel the thread.
        return failure instanceof InterruptedIOException
            && !(failure instanceof SocketTimeoutException);
    }

    private static void abort(FSDataOutputStream stream, Throwable failure) {
        boolean interrupted = Thread.currentThread().isInterrupted() || isInterruption(failure);
        try {
            final Abortable.AbortableResult result = stream.abort();
            if (result != null && result.anyCleanupException() != null
                    && result.anyCleanupException() != failure) {
                final IOException cleanupFailure = result.anyCleanupException();
                interrupted |= isInterruption(cleanupFailure);
                failure.addSuppressed(cleanupFailure);
            }
        } catch (RuntimeException | Error cleanupFailure) {
            if (cleanupFailure != failure) {
                failure.addSuppressed(cleanupFailure);
            }
        } finally {
            // Blocking I/O may clear the flag before throwing. Cleanup must not consume
            // cancellation, including an interruption encountered while aborting the upload.
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
