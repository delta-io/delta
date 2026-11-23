/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import com.google.common.base.Throwables;
import io.delta.storage.internal.GCSListing;
import io.delta.storage.internal.RecursiveListing;
import io.delta.storage.internal.ThreadUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * The {@link LogStore} implementation for GCS, which uses gcs-connector to
 * provide the necessary atomic and durability guarantees:
 *
 * <ol>
 *   <li>Atomic Visibility: Read/read-after-metadata-update/delete are strongly
 * consistent for GCS.</li>
 *
 *   <li>Consistent Listing: GCS guarantees strong consistency for both object and
 * bucket listing operations.
 * https://cloud.google.com/storage/docs/consistency</li>
 *
 *   <li>Mutual Exclusion: Preconditions are used to handle race conditions.</li>
 * </ol>
 *
 * Regarding file creation, this implementation:
 * <ul>
 *    <li>Opens a stream to write to GCS otherwise.</li>
 *    <li>Throws [[FileAlreadyExistsException]] if file exists and overwrite is false.</li>
 *    <li>Assumes file writing to be all-or-nothing, irrespective of overwrite option.</li>
 * </ul>
 * <p>
 * Regarding listing, when the gs:// FileSystem is gcs-connector 4.0.4+ (which provides
 * {@code GoogleHadoopFileSystem.listStatusStartingFrom}), {@code listFrom} sends the start name,
 * the directory boundary and skipped subtrees to GCS instead of listing the whole directory and
 * filtering client-side. The result is the same; the cost no longer grows with the number of
 * files before the start name (for example old commits in {@code _delta_log}) or inside
 * subdirectories (for example {@code _delta_log/_staged_commits}). Set
 * {@code delta.enableFastGCSListFrom=false} in the Hadoop configuration to use the previous
 * implementation.
 * <p>
 * This class is not meant for direct access but for configuration based on storage system.
 * See https://docs.delta.io/latest/delta-storage.html for details.
 */
public class GCSLogStore extends HadoopFileSystemLogStore {

    /** Hadoop conf key; server-side listing is on unless this is set to false. */
    public static final String FAST_LIST_FROM_CONF = "delta.enableFastGCSListFrom";

    final String preconditionFailedExceptionMessage = "412 Precondition Failed";

    private final boolean enableFastListFrom;

    public GCSLogStore(Configuration hadoopConf) {
        super(hadoopConf);
        this.enableFastListFrom =
            hadoopConf == null || hadoopConf.getBoolean(FAST_LIST_FROM_CONF, true);
    }

    /**
     * The server-side listing primitive for {@code fs}, or empty to use the previous
     * implementation. Protected so tests can supply one for a non-GCS FileSystem.
     */
    protected Optional<GCSListing.PageSource> fastListingSource(FileSystem fs) {
        // Local file systems are used by tests; they have no server-side listing.
        if (fs instanceof LocalFileSystem || fs instanceof RawLocalFileSystem) {
            return Optional.empty();
        }
        return GCSListing.forFileSystem(fs);
    }

    @Override
    public Iterator<FileStatus> listFrom(Path path, Configuration hadoopConf) throws IOException {
        final FileSystem fs = path.getFileSystem(hadoopConf);
        final Optional<GCSListing.PageSource> source =
            enableFastListFrom ? fastListingSource(fs) : Optional.empty();
        if (!source.isPresent()) {
            return super.listFrom(path, hadoopConf);
        }
        return fastListFrom(fs, source.get(), path, false, ListFilter.NONE);
    }

    @Override
    public Iterator<FileStatus> listFrom(
            Path path,
            boolean recursive,
            ListFilter filter,
            Configuration hadoopConf) throws IOException {
        final ListFilter effectiveFilter = filter == null ? ListFilter.NONE : filter;
        final FileSystem fs = path.getFileSystem(hadoopConf);
        final Optional<GCSListing.PageSource> source =
            enableFastListFrom ? fastListingSource(fs) : Optional.empty();
        if (!source.isPresent()) {
            // Builds on listFrom(Path, Configuration), which takes the previous implementation
            // for this FileSystem as well.
            return RecursiveListing.listFrom(this, path, recursive, effectiveFilter, hadoopConf);
        }
        return fastListFrom(fs, source.get(), path, recursive, effectiveFilter);
    }

    private Iterator<FileStatus> fastListFrom(
            FileSystem fs,
            GCSListing.PageSource source,
            Path path,
            boolean recursive,
            ListFilter filter) throws IOException {
        final Path resolvedPath = fs.makeQualified(path);
        final Path parentPath = resolvedPath.getParent();
        // A key listing of a missing directory is empty rather than an error. Probe explicitly to
        // keep listFrom's FileNotFoundException contract (an empty result also means "nothing at
        // or after the start name").
        if (!fs.exists(parentPath)) {
            throw new FileNotFoundException(
                String.format("No such file or directory: %s", parentPath));
        }
        return GCSListing.listFrom(source, resolvedPath, recursive, filter);
    }

    @Override
    public void write(
            Path path,
            Iterator<String> actions,
            Boolean overwrite,
            Configuration hadoopConf) throws IOException {
        final FileSystem fs = path.getFileSystem(hadoopConf);

        // This is needed for the tests to throw error with local file system.
        if (fs instanceof LocalFileSystem && !overwrite && fs.exists(path)) {
            throw new FileAlreadyExistsException(path.toString());
        }

        // GCS may upload an incomplete file when the current thread is interrupted, hence we move
        // the write to a new thread so that the write cannot be interrupted.
        // TODO Remove this hack when the GCS Hadoop connector fixes the issue.
        // If overwrite=false and path already exists, gcs-connector will throw
        // org.apache.hadoop.fs.FileAlreadyExistsException after fs.create is invoked.
        // This should be mapped to java.nio.file.FileAlreadyExistsException.
        Callable body = () -> {
            FSDataOutputStream stream = fs.create(path, overwrite);
            while (actions.hasNext()) {
                stream.write((actions.next() + "\n").getBytes(StandardCharsets.UTF_8));
            }
            stream.close();
            return "";
        };

        try {
            ThreadUtils.runInNewThread("delta-gcs-logstore-write", true, body);
        } catch (org.apache.hadoop.fs.FileAlreadyExistsException e) {
            throw new FileAlreadyExistsException(path.toString());
        } catch (IOException e) {
            // GCS uses preconditions to handle race conditions for multiple writers.
            // If path gets created between fs.create and stream.close by an external
            // agent or race conditions. Then this block will execute.
            // Reference: https://cloud.google.com/storage/docs/generations-preconditions
            if (isPreconditionFailure(e)) {
                if (!overwrite) {
                    throw new FileAlreadyExistsException(path.toString());
                }
            } else {
                throw e;
            }
        } catch (InterruptedException e) {
            InterruptedIOException iio = new InterruptedIOException(e.getMessage());
            iio.initCause(e);
            throw iio;
        } catch (Error | RuntimeException t) {
            throw t;
        } catch (Throwable t) {
            // Throw RuntimeException to avoid the calling interfaces from throwing Throwable
            throw new RuntimeException(t.getMessage(), t);
        }
    }

    private boolean isPreconditionFailure(Throwable x) {
        return Throwables.getCausalChain(x)
            .stream()
            .filter(p -> p != null)
            .filter(p -> p.getMessage() != null)
            .anyMatch(p -> p.getMessage().contains(preconditionFailedExceptionMessage));
    }

    @Override
    public Boolean isPartialWriteVisible(Path path, Configuration hadoopConf) throws IOException {
        return false;
    }
}
