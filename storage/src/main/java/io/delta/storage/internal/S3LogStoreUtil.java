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

import com.amazonaws.services.s3.model.ListObjectsV2Request;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Listing;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ListRequest;

import java.io.IOException;
import java.util.HashSet;

import static org.apache.hadoop.fs.s3a.S3AUtils.ACCEPT_ALL;
import static org.apache.hadoop.fs.s3a.S3AUtils.iteratorToStatuses;


/**
 * Static utility methods for the S3SingleDriverLogStore.
 *
 * Used to trick the class loader so we can use methods of org.apache.hadoop:hadoop-aws without needing to load this as
 * a dependency for tests in core.
 */
public class S3LogStoreUtil {
    private S3LogStoreUtil() {}

    /**
     * Uses the S3ListRequest.v2 interface with the startAfter parameter to only list files
     * which are lexicographically greater than resolvedPath.
     */
    public static FileStatus[] s3ListFrom(FileSystem fs, Path resolvedPath, Path parentPath) throws IOException {
        S3AFileSystem s3afs = (S3AFileSystem) fs;
        Listing listing = s3afs.getListing();
        // List files lexicographically after resolvedPath inclusive within the same directory
        RemoteIterator<S3AFileStatus> l = listing.createFileStatusListingIterator(resolvedPath,
                S3ListRequest.v2(
                        new ListObjectsV2Request()
                                .withBucketName(s3afs.getBucket())
                                .withMaxKeys(1000)
                                .withPrefix(s3afs.pathToKey(parentPath))
                                .withStartAfter(keyBefore(s3afs.pathToKey(resolvedPath)))
                ), ACCEPT_ALL,
                new Listing.AcceptAllButSelfAndS3nDirs(parentPath)
        );
        return iteratorToStatuses(l, new HashSet<>());
    }

    /**
     * Subtract one from the last byte of key to get the key which is lexicographically right before key.
     */
    static String keyBefore(String key) {
        byte[] bytes = key.getBytes();
        if(bytes.length == 0 || bytes[bytes.length - 1] <= 1) throw new IllegalArgumentException("Empty or invalid key: " + key);
        bytes[bytes.length-1] -= 1;
        return new String(bytes);
    }
}
