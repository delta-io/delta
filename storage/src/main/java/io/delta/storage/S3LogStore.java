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

package io.delta.storage;

import java.io.IOException;
import java.util.Iterator;

import io.delta.storage.internal.S3ConditionalWrite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.Options.CreateFileOptionKeys
    .FS_OPTION_CREATE_CONDITIONAL_OVERWRITE;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_HEADER;

/**
 * S3 LogStore using native conditional publication for mutual exclusion across drivers.
 *
 * <p>Requires Hadoop S3A 3.4.2 or later, conditional creation, user metadata and abortable,
 * immediately visible output streams. Unsupported configurations fail the write. All writers
 * must use a compatible atomic publication protocol and preserve immutable destination keys
 * during recovery. Mixing this store with unconditional writers in independent drivers is
 * unsafe. Native and legacy Java S3 LogStores share a per-path lock within a JVM.</p>
 *
 * <p>A lost acknowledgement is recovered when the destination carries this invocation's UUID.
 * Inconclusive recovery throws {@link S3UnknownWriteOutcomeException}, or
 * {@link java.io.InterruptedIOException} on cancellation. The write may already have committed.
 * Neither whole-upload replay nor process-restart recovery is provided.</p>
 */
public class S3LogStore extends BaseS3LogStore {

    public S3LogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    /**
     * Whether this filesystem advertises the prerequisites for native writes. Selection must
     * use the operation's actual filesystem and path, not cache this result by URI scheme.
     * The writer also checks classloader compatibility and validates the opened stream.
     * An I/O or linkage failure is not evidence that unconditional writing is safe.
     */
    public static boolean hasNativeWriteCapabilities(FileSystem fs, Path path) throws IOException {
        // Older Hadoop versions have no filesystem capability API.
        return S3AClassLoaderSupport.HAS_PATH_CAPABILITY
            && hasS3ASupertype(fs.getClass())
            && fs.hasPathCapability(path, FS_OPTION_CREATE_CONDITIONAL_OVERWRITE)
            && fs.hasPathCapability(path, FS_S3A_CREATE_HEADER);
    }

    /** Checks native-write capabilities and S3A classloader compatibility for recovery. */
    public static boolean isSupported(FileSystem fs, Path path) throws IOException {
        if (!hasNativeWriteCapabilities(fs, path)) {
            return false;
        }

        final boolean compatibleClass = S3AClassLoaderSupport.S3A_CLASS != null
            && S3AClassLoaderSupport.S3A_CLASS.isInstance(fs);
        if (!compatibleClass) {
            // Hadoop can load S3A through a classloader that Delta cannot use for recovery.
            // Falling back here would drop coordination between drivers.
            throw new IOException(
                "The S3A filesystem and delta-storage use incompatible classloaders. "
                    + "Load delta-storage and hadoop-aws with compatible classloaders.");
        }
        return true;
    }

    private static boolean hasS3ASupertype(Class<?> type) {
        for (Class<?> current = type; current != null; current = current.getSuperclass()) {
            if (S3AClassLoaderSupport.S3A_CLASS_NAME.equals(current.getName())) {
                return true;
            }
        }
        return false;
    }

    /** Hadoop APIs and S3A types visible to delta-storage's classloader. */
    private static final class S3AClassLoaderSupport {
        private static final String S3A_CLASS_NAME = "org.apache.hadoop.fs.s3a.S3AFileSystem";
        private static final boolean HAS_PATH_CAPABILITY = hasPathCapabilityMethod();
        private static final Class<?> S3A_CLASS = HAS_PATH_CAPABILITY ? findS3AClass() : null;

        private static boolean hasPathCapabilityMethod() {
            try {
                FileSystem.class.getMethod("hasPathCapability", Path.class, String.class);
                return true;
            } catch (NoSuchMethodException unavailable) {
                return false;
            }
        }

        private static Class<?> findS3AClass() {
            try {
                return Class.forName(S3A_CLASS_NAME, false, S3LogStore.class.getClassLoader());
            } catch (ClassNotFoundException unavailable) {
                return null;
            }
        }
    }

    @Override
    public void write(
            Path path,
            Iterator<String> actions,
            Boolean overwrite,
            Configuration hadoopConf) throws IOException {
        if (overwrite) {
            // writeWithPathLock does not request native put-if-absent.
            // We use it for overwrite=true so an existing object can be replaced.
            writeWithPathLock(path, actions, true, hadoopConf);
        } else {
            final FileSystem fs = path.getFileSystem(hadoopConf);
            final Path resolvedPath = resolvePathWithoutUserInfo(fs, path);
            // Check before loading the S3A-dependent writer. Reads do not need these classes.
            if (!isSupported(fs, resolvedPath)) {
                throw new UnsupportedOperationException(
                    "Native S3 LogStore requires S3A conditional creation and user metadata: "
                        + resolvedPath);
            }
            withPathLock(resolvedPath, () -> S3ConditionalWrite.write(fs, resolvedPath, actions));
        }
    }
}
