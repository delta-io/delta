/*
 * Copyright (2022) The Delta Lake Project Authors.
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

import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.s3a.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;

import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_MAX_PAGING_KEYS;
import static org.apache.hadoop.fs.s3a.Constants.MAX_PAGING_KEYS;
import static org.apache.hadoop.fs.s3a.S3AUtils.iteratorToStatuses;
import static org.apache.hadoop.fs.s3a.S3AUtils.maybeAddTrailingSlash;


/**
 * Static utility methods for the S3SingleDriverLogStore.
 *
 * Used to trick the class loader so we can use methods of org.apache.hadoop:hadoop-aws without needing to load this as
 * a dependency for tests in core.
 */
public final class S3LogStoreUtil {
    private S3LogStoreUtil() {}

    private static PathFilter ACCEPT_ALL = new PathFilter() {
        @Override
        public boolean accept(Path file) {
            return true;
        }

        @Override
        public String toString() {
            return "ACCEPT_ALL";
        }
    };

    /**
     * Uses the S3ListRequest.v2 interface with the startAfter parameter to only list files
     * which are lexicographically greater than resolvedPath.
     */
    private static RemoteIterator<S3AFileStatus> s3ListFrom(
            S3AFileSystem s3afs,
            Path resolvedPath,
            Path parentPath) throws IOException {
        int maxKeys = S3AUtils.intOption(s3afs.getConf(), MAX_PAGING_KEYS, DEFAULT_MAX_PAGING_KEYS, 1);
        Listing listing = s3afs.getListing();
        // List files lexicographically after resolvedPath inclusive within the same directory
        return listing.createFileStatusListingIterator(resolvedPath,
                S3ListRequest.v2(
                    buildListObjectsV2Request(
                        s3afs.getBucket(),
                        s3afs.pathToKey(parentPath),
                        s3afs.pathToKey(resolvedPath),
                        maxKeys)
                ), ACCEPT_ALL,
                new Listing.AcceptAllButSelfAndS3nDirs(parentPath),
                s3afs.getActiveAuditSpan());
    }

    /**
     * Builds the {@link ListObjectsV2Request} for the fast {@code listFrom} path.
     *
     * <p>{@code delimiter="/"} keeps the listing to the immediate children of {@code parentPath}
     * (matching {@code FileSystem.listStatus}); without it ListObjectsV2 recurses and surfaces
     * {@code _staged_commits/} files as if they were commits. The delimiter only groups
     * subdirectories when the prefix ends in {@code "/"}, so we re-add the trailing slash that
     * {@code pathToKey} strips via {@code maybeAddTrailingSlash}.
     *
     * <p>Package-private for unit testing.
     */
    static ListObjectsV2Request buildListObjectsV2Request(
            String bucket,
            String parentKey,
            String resolvedKey,
            int maxKeys) {
        return ListObjectsV2Request.builder()
            .bucket(bucket)
            .maxKeys(maxKeys)
            .prefix(maybeAddTrailingSlash(parentKey))
            .delimiter("/")
            .startAfter(keyBefore(resolvedKey))
            .build();
    }

    /**
     * Uses the S3ListRequest.v2 interface with the startAfter parameter to only list files
     * which are lexicographically greater than resolvedPath.
     *
     * Wraps s3ListFrom in an array. Contained in this class to avoid contaminating other
     * classes with dependencies on recent Hadoop versions.
     *
     * TODO: Remove this method when iterators are used everywhere.
     */
    public static FileStatus[] s3ListFromArray(
            FileSystem fs,
            Path resolvedPath,
            Path parentPath) throws IOException {
        S3AFileSystem s3afs;
        try {
            s3afs = (S3AFileSystem) unwrap(fs);
        } catch (ClassCastException e) {
            throw new UnsupportedOperationException(
                    "The Hadoop file system used for the S3LogStore must be castable to " +
                            "org.apache.hadoop.fs.s3a.S3AFileSystem.", e);
        }
        return iteratorToStatuses(S3LogStoreUtil.s3ListFrom(s3afs, resolvedPath, parentPath));
    }

    /**
     * Unwraps {@link FilterFileSystem} wrappers that delegate to a real {@link S3AFileSystem}.
     */
    static FileSystem unwrap(FileSystem fs) {
        while (!(fs instanceof S3AFileSystem) && fs instanceof FilterFileSystem) {
            fs = ((FilterFileSystem) fs).getRawFileSystem();
        }
        return fs;
    }

    /**
     * Get the key which is lexicographically right before key.
     * If the key is empty return null.
     * If the key ends in a null byte, remove the last byte.
     * Otherwise, subtract one from the last byte.
     */
    static String keyBefore(String key) {
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        if(bytes.length == 0) return null;
        if(bytes[bytes.length - 1] > 0) {
            bytes[bytes.length - 1] -= 1;
            return new String(bytes, StandardCharsets.UTF_8);
        } else {
            return new String(bytes, 0, bytes.length - 1, StandardCharsets.UTF_8);
        }
    }
}
