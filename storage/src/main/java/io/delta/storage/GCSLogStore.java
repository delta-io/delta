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
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

/**
 * :: Unstable ::
 * <p>
 * The [[LogStore]] implementation for GCS, which uses gcs-connector to
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
 * Regarding file creation, this implementation:
 * <ul>
 *    <li>Opens a stream to write to GCS otherwise.</li>
 *    <li>Throws [[FileAlreadyExistsException]] if file exists and overwrite is false.</li>
 *    <li>Assumes file writing to be all-or-nothing, irrespective of overwrite option.</li>
 * </ul>
 *
 * @note This class is not meant for direct access but for configuration based on storage system.
 * See https://docs.delta.io/latest/delta-storage.html for details.
 */
@InterfaceStability.Unstable
public class GCSLogStore extends HadoopFileSystemLogStore {

    String preconditionFailedExceptionMessage = "412 Precondition Failed";

    public GCSLogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    private static <T> T runInNewThread(
            String threadName,
            Boolean isDaemon,
            Callable<T> body) throws Exception {
        List<Exception> exceptionHolder = new ArrayList<>(1);
        List<T> resultHolder = new ArrayList<>(1);
        Thread thread = new Thread(threadName) {
            @Override
            public void run() {
                try {
                    resultHolder.add(body.call());
                } catch (Exception ex) {
                    exceptionHolder.add(ex);
                }
            }
        };
        thread.setDaemon(isDaemon);
        thread.start();
        thread.join();
        if (!exceptionHolder.isEmpty()) {
            Exception realException = exceptionHolder.get(0);
            // Remove the part of the stack that shows method calls into this helper method
            // This means drop everything from the top until the stack element
            // ThreadUtils.runInNewThread(), and then drop that as well (hence the `drop(1)`).
            Stream<StackTraceElement> baseStackTrace = Arrays.stream(
                    Thread.currentThread().getStackTrace()).dropWhile(t ->
                    !t.getClassName().contains(GCSLogStore.class.getSimpleName())).skip(1);
            // Remove the part of the new thread stack that shows methods call from this helper method
            Stream<StackTraceElement> extraStackTrace = Arrays.stream(
                    realException.getStackTrace()).takeWhile(e ->
                    !e.getClassName().contains(GCSLogStore.class.getSimpleName()));
            // Combine the two stack traces, with a place holder just specifying that there
            // was a helper method used, without any further details of the helper
            Stream<StackTraceElement> placeHolderStackElem = Stream.of(
                    new StackTraceElement(
                            String.format("... run in separate thread using $s", GCSLogStore.class.getSimpleName(),
                                    " static method runInNewThread"),
                            "",
                            "",
                            -1)
            );
            StackTraceElement[] finalStackTrace = Stream.concat(
                    Stream.concat(baseStackTrace,
                            placeHolderStackElem),
                    baseStackTrace).toArray(StackTraceElement[]::new);
            realException.setStackTrace(finalStackTrace);
            throw realException;
        } else {
            return resultHolder.get(0);
        }
    }

    @Override
    public void write(Path path,
                      Iterator<String> actions,
                      Boolean overwrite,
                      Configuration hadoopConf) throws IOException {
        FileSystem fs = path.getFileSystem(hadoopConf);

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
        FSDataOutputStream stream = fs.create(path, overwrite);

        Callable body = () -> {
            try {
                while (actions.hasNext()) {
                    stream.write((actions.next() + "\n").getBytes(StandardCharsets.UTF_8));
                }
            } finally {
                stream.close();

            }
            return "";
        };

        try {
            runInNewThread("delta-gcs-logstore-write", true, body);
        } catch (org.apache.hadoop.fs.FileAlreadyExistsException e) {
            throw new FileAlreadyExistsException(path.toString());
            // GCS uses preconditions to handle race conditions for multiple writers.
            // If path gets created between fs.create and stream.close by an external
            // agent or race conditions. Then this block will execute.
            // Reference: https://cloud.google.com/storage/docs/generations-preconditions
        } catch (IOException e) {
            if (isPreconditionFailure(e) && !overwrite)
                throw new FileAlreadyExistsException(path.toString());
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

    }

    private Boolean isPreconditionFailure(Throwable x) {
        return Throwables.getCausalChain(x)
                .stream()
                .filter(p -> p != null)
                .filter(p -> p.getMessage() != null)
                .filter(p -> p.getMessage().contains(preconditionFailedExceptionMessage))
                .findFirst()
                .isPresent();
    }

    @Override
    public Boolean isPartialWriteVisible(Path path, Configuration hadoopConf) throws IOException {
        return null;
    }
}
