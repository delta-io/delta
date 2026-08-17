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
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * Static utility methods for the GCSLogStore.
 *
 * Isolates the dependency on com.google.cloud:google-cloud-storage so that GCSLogStore can be
 * loaded without it on the classpath as long as delta.enableFastGCSListFrom stays disabled.
 */
public final class GCSLogStoreUtil {
    private GCSLogStoreUtil() {}

    /**
     * The GCS client authenticates via Application Default Credentials (which also honors
     * GOOGLE_APPLICATION_CREDENTIALS), independently of any fs.gs.auth.* Hadoop configuration
     * the gcs-connector may use: the connector's shaded client cannot be reused here because
     * its google-cloud-storage classes are relocated.
     */
    private static volatile Storage storage;

    private static Storage getStorage() {
        if (storage == null) {
            synchronized (GCSLogStoreUtil.class) {
                if (storage == null) {
                    storage = StorageOptions.getDefaultInstance().getService();
                }
            }
        }
        return storage;
    }

    /**
     * Lists files which are lexicographically equal to or after resolvedPath within its
     * parent directory, using the GCS startOffset list parameter — O(matching files) instead
     * of the O(directory) fs.listStatus in HadoopFileSystemLogStore.listFrom. Unlike S3's
     * exclusive startAfter, GCS's startOffset is inclusive, so no "key just before" trick
     * is needed.
     */
    public static FileStatus[] gcsListFromArray(
            Path resolvedPath,
            Path parentPath) throws IOException {
        final URI uri = resolvedPath.toUri();
        if (!"gs".equalsIgnoreCase(uri.getScheme()) || uri.getAuthority() == null) {
            throw new UnsupportedOperationException(
                "The fast GCS list path (delta.enableFastGCSListFrom) requires an absolute " +
                    "gs://bucket/path URI. Got: " + resolvedPath);
        }
        return gcsListFrom(getStorage(), uri.getAuthority(), resolvedPath, parentPath);
    }

    /**
     * Overload taking an explicit {@link Storage} client. Visible for tests (e.g.
     * GCSLogStoreUtilIntegrationTest instruments the client to observe list requests);
     * the production entry point is {@link #gcsListFromArray(Path, Path)}.
     */
    public static FileStatus[] gcsListFrom(
            Storage storage,
            String bucket,
            Path resolvedPath,
            Path parentPath) throws IOException {
        String dirKey = pathToKey(parentPath);
        if (!dirKey.isEmpty() && !dirKey.endsWith("/")) {
            dirKey = dirKey + "/";
        }
        final Iterable<Blob> blobs;
        try {
            blobs = storage.list(
                bucket,
                Storage.BlobListOption.prefix(dirKey),
                Storage.BlobListOption.startOffset(pathToKey(resolvedPath)),
                // Delimiter listing: only direct children come back; subdirectories appear
                // as directory pseudo-blobs — the same shape FileSystem.listStatus produces.
                Storage.BlobListOption.currentDirectory(),
                Storage.BlobListOption.fields(
                    Storage.BlobField.NAME, Storage.BlobField.SIZE, Storage.BlobField.UPDATED)
            ).iterateAll();
        } catch (com.google.cloud.storage.StorageException e) {
            throw new IOException(
                String.format("Failed to list gs://%s/%s", bucket, dirKey), e);
        }
        final List<FileStatus> statuses = new ArrayList<>();
        for (Blob blob : blobs) {
            statuses.add(blobToFileStatus(blob, parentPath));
        }
        return statuses.toArray(new FileStatus[0]);
    }

    static FileStatus blobToFileStatus(BlobInfo blob, Path parentPath) {
        return toFileStatus(
            blob.getName(),
            blob.isDirectory(),
            blob.getSize(),
            blob.getUpdateTimeOffsetDateTime(),
            parentPath);
    }

    static FileStatus toFileStatus(
            String key,
            boolean isDir,
            Long size,
            java.time.OffsetDateTime updated,
            Path parentPath) {
        // Directory pseudo-blobs come back as their key prefix, e.g. "a/_delta_log/subdir/".
        if (isDir && key.endsWith("/")) {
            key = key.substring(0, key.length() - 1);
        }
        final int lastSlash = key.lastIndexOf('/');
        final String childName = lastSlash >= 0 ? key.substring(lastSlash + 1) : key;
        final long length = size == null ? 0L : size;
        final long modificationTime = updated == null ? 0L : updated.toInstant().toEpochMilli();
        return new FileStatus(
            length,
            isDir,
            1, // block_replication
            0, // blocksize
            modificationTime,
            new Path(parentPath, childName));
    }

    /**
     * Object key of {@code path} within its bucket, i.e. the URI path without the
     * leading slash.
     */
    static String pathToKey(Path path) {
        final String key = path.toUri().getPath();
        return key.startsWith("/") ? key.substring(1) : key;
    }
}
