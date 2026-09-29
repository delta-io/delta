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

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.Set;

/**
 * Retains a byte sequence for replay, spilling to an owner-only temporary file past a configured
 * memory limit.
 */
final class S3WriteReplayBuffer implements Closeable {

    private static final Set<PosixFilePermission> OWNER_ONLY_PERMISSIONS =
        EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);

    private final int memoryLimitBytes;
    private final Path tempDirectory;
    private final ByteArrayOutputStream memory = new ByteArrayOutputStream();

    private Path spillPath;
    private OutputStream spillStream;
    private boolean sealed;
    private boolean closed;

    S3WriteReplayBuffer(int memoryLimitBytes) {
        this(memoryLimitBytes, Paths.get(System.getProperty("java.io.tmpdir")));
    }

    S3WriteReplayBuffer(int memoryLimitBytes, Path tempDirectory) {
        if (memoryLimitBytes < 0) {
            throw new IllegalArgumentException("memoryLimitBytes must not be negative");
        }
        this.memoryLimitBytes = memoryLimitBytes;
        this.tempDirectory = tempDirectory;
    }

    void write(byte[] bytes) throws IOException {
        ensureWritable();
        if (spillStream == null && memory.size() + bytes.length > memoryLimitBytes) {
            spillToDisk();
        }
        if (spillStream != null) {
            spillStream.write(bytes);
        } else {
            memory.write(bytes);
        }
    }

    void seal() throws IOException {
        ensureOpen();
        if (sealed) {
            return;
        }
        if (spillStream != null) {
            spillStream.close();
            spillStream = null;
        }
        sealed = true;
    }

    void replayTo(OutputStream destination) throws IOException {
        ensureOpen();
        if (!sealed) {
            throw new IllegalStateException("Replay buffer must be sealed before replay");
        }
        if (spillPath != null) {
            Files.copy(spillPath, destination);
        } else {
            memory.writeTo(destination);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        IOException failure = null;
        if (spillStream != null) {
            try {
                spillStream.close();
            } catch (IOException e) {
                failure = e;
            } finally {
                spillStream = null;
            }
        }
        if (spillPath != null) {
            try {
                Files.deleteIfExists(spillPath);
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            } finally {
                spillPath = null;
            }
        }
        memory.reset();
        if (failure != null) {
            throw failure;
        }
    }

    private void spillToDisk() throws IOException {
        Path newSpillPath = createSecureTempFile(tempDirectory);
        OutputStream newSpillStream = null;
        try {
            newSpillStream = new BufferedOutputStream(Files.newOutputStream(newSpillPath));
            memory.writeTo(newSpillStream);
            memory.reset();
            spillPath = newSpillPath;
            spillStream = newSpillStream;
        } catch (IOException | RuntimeException e) {
            if (newSpillStream != null) {
                try {
                    newSpillStream.close();
                } catch (IOException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
            }
            try {
                Files.deleteIfExists(newSpillPath);
            } catch (IOException deleteFailure) {
                e.addSuppressed(deleteFailure);
            }
            throw e;
        }
    }

    private static Path createSecureTempFile(Path directory) throws IOException {
        try {
            return Files.createTempFile(
                directory,
                "delta-s3-write-",
                ".tmp",
                PosixFilePermissions.asFileAttribute(OWNER_ONLY_PERMISSIONS));
        } catch (UnsupportedOperationException e) {
            Path path = Files.createTempFile(directory, "delta-s3-write-", ".tmp");
            File file = path.toFile();
            boolean secured =
                file.setReadable(false, false)
                    && file.setWritable(false, false)
                    && file.setExecutable(false, false)
                    && file.setReadable(true, true)
                    && file.setWritable(true, true);
            if (!secured) {
                Files.deleteIfExists(path);
                throw new IOException("Unable to restrict replay buffer permissions", e);
            }
            return path;
        }
    }

    private void ensureWritable() {
        ensureOpen();
        if (sealed) {
            throw new IllegalStateException("Replay buffer is sealed");
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Replay buffer is closed");
        }
    }
}
