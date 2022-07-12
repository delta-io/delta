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
                                .withStartAfter(s3afs.pathToKey(resolvedPath))
                ), ACCEPT_ALL,
                new Listing.AcceptAllButSelfAndS3nDirs(resolvedPath)
        );
        return iteratorToStatuses(l, new HashSet<>());
    }
}
