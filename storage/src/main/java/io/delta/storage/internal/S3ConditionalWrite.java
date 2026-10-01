/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;

import org.apache.hadoop.fs.Abortable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.s3a.AWSServiceIOException;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.RemoteFileChangedException;

import static org.apache.hadoop.fs.Options.CreateFileOptionKeys
    .FS_OPTION_CREATE_CONDITIONAL_OVERWRITE;

/**
 * Writes an S3 object with atomic create-if-absent semantics and reconciles ambiguous close
 * failures using a durable per-write owner identifier.
 */
public final class S3ConditionalWrite {

    static final String WRITE_ID_METADATA_KEY = "delta-log-store-write-id";
    static final String WRITE_ID_HEADER_OPTION =
        Constants.FS_S3A_CREATE_HEADER + "." + WRITE_ID_METADATA_KEY;
    static final String WRITE_ID_XATTR = Constants.XA_HEADER_PREFIX + WRITE_ID_METADATA_KEY;

    private S3ConditionalWrite() {}

    /**
     * Write every action to the destination, with one UTF-8 newline after each action.
     */
    public static void write(
            FileSystem fs,
            Path path,
            Iterator<String> actions) throws IOException {
        final String writeId = UUID.randomUUID().toString();
        final FSDataOutputStream stream = createConditionalStream(fs, path, writeId);
        try {
            requireAbortable(stream, path);
        } catch (RuntimeException failure) {
            abortAfterFailure(stream, failure);
            throw failure;
        }

        try {
            writeActions(actions, stream);
        } catch (IOException | RuntimeException | Error failure) {
            abortAfterFailure(stream, failure);
            throw failure;
        }
        closeConditionalWrite(fs, path, writeId, stream);
    }

    private static FSDataOutputStream createConditionalStream(
            FileSystem fs,
            Path path,
            String writeId) throws IOException {
        final FSDataOutputStreamBuilder<?, ?> builder = fs.createFile(path);
        builder.overwrite(false);
        builder.must(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE, true);
        builder.must(WRITE_ID_HEADER_OPTION, writeId);
        return builder.build();
    }

    private static void writeActions(
            Iterator<String> actions,
            FSDataOutputStream stream) throws IOException {
        while (actions.hasNext()) {
            final byte[] line =
                (actions.next() + "\n").getBytes(StandardCharsets.UTF_8);
            stream.write(line);
        }
    }

    private static void closeConditionalWrite(
            FileSystem fs,
            Path path,
            String writeId,
            FSDataOutputStream stream) throws IOException {
        try {
            stream.close();
        } catch (IOException failure) {
            reconcileConditionalFailure(fs, path, writeId, failure);
        }
    }

    private static void reconcileConditionalFailure(
            FileSystem fs,
            Path path,
            String writeId,
            IOException failure) throws IOException {
        final byte[] storedWriteId;
        try {
            storedWriteId = fs.getXAttr(path, WRITE_ID_XATTR);
        } catch (FileNotFoundException notFound) {
            failure.addSuppressed(notFound);
            throw failure;
        } catch (IOException | UnsupportedOperationException reconciliationFailure) {
            failure.addSuppressed(reconciliationFailure);
            throw failure;
        }

        if (Arrays.equals(writeId.getBytes(StandardCharsets.UTF_8), storedWriteId)) {
            return;
        }

        if (isConditionalConflict(failure)) {
            final FileAlreadyExistsException conflict =
                new FileAlreadyExistsException(path.toString());
            conflict.initCause(failure);
            throw conflict;
        }

        throw failure;
    }

    private static void requireAbortable(FSDataOutputStream stream, Path path) {
        if (!stream.hasCapability(StreamCapabilities.ABORTABLE_STREAM)) {
            throw new UnsupportedOperationException(
                "S3 conditional writes require an abortable output stream: " + path);
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

    private static boolean isConditionalRequestConflict(IOException failure) {
        return failure instanceof AWSServiceIOException
            && ((AWSServiceIOException) failure).statusCode() == 409;
    }

    private static boolean isConditionalConflict(IOException failure) {
        return failure instanceof RemoteFileChangedException
            || failure instanceof org.apache.hadoop.fs.FileAlreadyExistsException
            || (failure instanceof AWSServiceIOException
                && ((AWSServiceIOException) failure).statusCode() == 412)
            || isConditionalRequestConflict(failure);
    }

}
