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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;

/**
 * Retains a write so an S3 conditional-request conflict can replay the complete upload.
 *
 * <p>Small writes stay in memory. Larger writes spill to an owner-only temporary file because a
 * Delta commit can contain enough actions that retaining every byte on the driver heap is unsafe.
 * The buffer must be sealed before replay so a retry cannot observe an incomplete spill file.</p>
 */
final class S3WriteReplayBuffer implements AutoCloseable {

    private static final int COPY_BUFFER_SIZE = 64 * 1024;
    private static final FileAttribute<?>[] NO_FILE_ATTRIBUTES = new FileAttribute<?>[0];
    private static final FileAttribute<?>[] OWNER_ONLY_FILE_ATTRIBUTES = new FileAttribute<?>[] {
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"))
    };

    private final int memoryLimitBytes;
    private final ByteArrayOutputStream memory;
    private Path spillFile;
    private OutputStream spillOutput;
    private boolean sealed;
    private boolean closed;

    S3WriteReplayBuffer(int memoryLimitBytes) {
        if (memoryLimitBytes < 0) {
            throw new IllegalArgumentException("memoryLimitBytes must not be negative");
        }
        this.memoryLimitBytes = memoryLimitBytes;
        this.memory = new ByteArrayOutputStream(Math.min(memoryLimitBytes, COPY_BUFFER_SIZE));
    }

    void write(byte[] bytes) throws IOException {
        if (sealed || closed) {
            throw new IllegalStateException("Cannot append to a sealed or closed replay buffer");
        }

        if (spillOutput == null && bytes.length > memoryLimitBytes - memory.size()) {
            spillToDisk();
        }

        if (spillOutput == null) {
            memory.write(bytes);
        } else {
            spillOutput.write(bytes);
        }
    }

    void seal() throws IOException {
        if (closed) {
            throw new IllegalStateException("Cannot seal a closed replay buffer");
        }
        if (!sealed) {
            if (spillOutput != null) {
                spillOutput.close();
                spillOutput = null;
            }
            sealed = true;
        }
    }

    void replayTo(OutputStream destination) throws IOException {
        if (!sealed || closed) {
            throw new IllegalStateException("Replay requires a sealed, open buffer");
        }

        if (spillFile == null) {
            memory.writeTo(destination);
            return;
        }

        try (InputStream input = new BufferedInputStream(Files.newInputStream(spillFile))) {
            final byte[] copyBuffer = new byte[COPY_BUFFER_SIZE];
            int count;
            while ((count = input.read(copyBuffer)) >= 0) {
                destination.write(copyBuffer, 0, count);
            }
        }
    }

    boolean hasSpilledToDisk() {
        return spillFile != null;
    }

    Path spillFile() {
        return spillFile;
    }

    private void spillToDisk() throws IOException {
        final FileAttribute<?>[] attributes =
            FileSystems.getDefault().supportedFileAttributeViews().contains("posix")
                ? OWNER_ONLY_FILE_ATTRIBUTES
                : NO_FILE_ATTRIBUTES;
        spillFile = Files.createTempFile("delta-s3-logstore-", ".replay", attributes);
        spillOutput = new BufferedOutputStream(Files.newOutputStream(spillFile));
        memory.writeTo(spillOutput);
        memory.reset();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        IOException failure = null;
        if (spillOutput != null) {
            try {
                spillOutput.close();
            } catch (IOException closeFailure) {
                failure = closeFailure;
            } finally {
                spillOutput = null;
            }
        }

        memory.reset();
        if (spillFile != null) {
            try {
                Files.deleteIfExists(spillFile);
            } catch (IOException deleteFailure) {
                // Register the path only after immediate deletion fails. The JDK retains every
                // deleteOnExit path until JVM shutdown, so registering successful spills would
                // grow driver memory for the lifetime of the process.
                try {
                    spillFile.toFile().deleteOnExit();
                } catch (RuntimeException fallbackFailure) {
                    // Preserve the immediate deletion failure as the primary cleanup signal.
                    deleteFailure.addSuppressed(fallbackFailure);
                }
                if (failure == null) {
                    failure = deleteFailure;
                } else {
                    failure.addSuppressed(deleteFailure);
                }
            }
        }

        if (failure != null) {
            throw failure;
        }
    }
}
