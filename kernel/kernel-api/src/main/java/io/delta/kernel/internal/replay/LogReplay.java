/*
 * Copyright (2023) The Delta Lake Project Authors.
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

package io.delta.kernel.internal.replay;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.*;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;

import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.util.Logging;

/**
 * Replays a history of actions, resolving them to produce the current state of the table. The
 * protocol for resolution is as follows:
 *  - The most recent {@link AddFile} and accompanying metadata for any `(path, dv id)` tuple wins.
 *  - {@link RemoveFile} deletes a corresponding AddFile. A RemoveFile "corresponds" to the AddFile
 *    that matches both the parquet file URI *and* the deletion vector's URI (if any).
 *  - The most recent {@link Metadata} wins.
 *  - The most recent {@link Protocol} version wins.
 *  - For each `(path, dv id)` tuple, this class should always output only one {@link FileAction}
 *    (either AddFile or RemoveFile)
 *
 * This class exposes only two public APIs
 * - {@link #loadProtocolAndMetadata()}: replay the log in reverse and return the first non-null
 *                                       Protocol and Metadata
 * - {@link #getAddFilesAsColumnarBatches}: return all active (not tombstoned) AddFiles as
 *                                          {@link ColumnarBatch}s
 */
public class LogReplay implements Logging {

    /** Read schema when searching for the latest Protocol and Metadata. */
    public static final StructType PROTOCOL_METADATA_READ_SCHEMA = new StructType()
        .add("protocol", Protocol.READ_SCHEMA)
        .add("metaData", Metadata.READ_SCHEMA);

    /**
     * Read schema when searching for all the active AddFiles (need some RemoveFile info, too).
     * Note that we don't need to read the entire RemoveFile, only the path and dv info.
     */
    public static final StructType ADD_REMOVE_READ_SCHEMA = new StructType()
        .add("add", AddFile.READ_SCHEMA)
        .add("remove", new StructType()
            .add("path", StringType.INSTANCE, false /* nullable */)
            .add("deletionVector", DeletionVectorDescriptor.READ_SCHEMA, true /* nullable */)
        );

    /** Data (result) schema of the remaining active AddFiles. */
    public static final StructType ADD_ONLY_DATA_SCHEMA = new StructType()
        .add("add", AddFile.READ_SCHEMA);

    private final Path dataPath;
    private final LogSegment logSegment;
    private final TableClient tableClient;

    private final Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata;

    public LogReplay(
            Path logPath,
            Path dataPath,
            TableClient tableClient,
            LogSegment logSegment) {
        assertLogFilesBelongToTable(logPath, logSegment.allLogFilesUnsorted());

        this.dataPath = dataPath;
        this.logSegment = logSegment;
        this.tableClient = tableClient;
        this.protocolAndMetadata = new Lazy<>(this::loadTableProtocolAndMetadata);
    }

    /////////////////
    // Public APIs //
    /////////////////

    public Tuple2<Protocol, Metadata> loadProtocolAndMetadata() {
        return this.protocolAndMetadata.get();
    }

    /**
     * Returns an iterator of {@link DataReadResult} with schema {@link #ADD_ONLY_DATA_SCHEMA}
     * representing all the active AddFiles in the table (passing the DataReadResult's selection
     * vector means it is active).
     */
    public CloseableIterator<DataReadResult> getAddFilesAsColumnarBatches() {
        final CloseableIterator<Tuple2<FileDataReadResult, Boolean>> addRemoveIter =
            new ActionsIterator(
                tableClient,
                logSegment.allLogFilesReversed(),
                ADD_REMOVE_READ_SCHEMA);

        return new ActiveAddFilesIterator(addRemoveIter);
    }

    /**
     * TEMPORARY. DELETE THIS. We should only expose AddFiles as part of ColumnarBatches via
     * getAddFilesAsColumnarBatches. That requires updating downstream consumers of this API.
     *
     * TODO: delete this API and update downstream consumers to use getAddFilesAsColumnarBatches
     *       instead.
     *
     * Converts the iterator of ColumnarBatches of AddFiles into an iterator of deserialized AddFile
     * actions.
     */
    public CloseableIterator<AddFile> getAddFiles() {
        return new CloseableIterator<AddFile>() {
            private final CloseableIterator<DataReadResult> batchesIter =
                getAddFilesAsColumnarBatches();

            // empty - not yet initialized
            // not empty - exists a current batch
            private Optional<CloseableIterator<Row>> currBatchIter = Optional.empty();

            private Optional<ColumnVector> currBatchSelectionVector = Optional.empty();

            private int currBatchRowIndex = -1;

            // empty - not yet initialized
            // not empty - exists a next AddFile to return
            private Optional<AddFile> nextAddFile = Optional.empty();

            @Override
            public boolean hasNext() {
                if (!nextAddFile.isPresent()) {
                    tryLoadNextAddFile();
                }

                return nextAddFile.isPresent();
            }

            @Override
            public AddFile next() {
                if (!hasNext()) throw new NoSuchElementException();

                final AddFile ret = nextAddFile.get();
                nextAddFile = Optional.empty();
                return ret;
            }

            @Override
            public void close() throws IOException {
                batchesIter.close();

                if (currBatchIter.isPresent()) {
                    currBatchIter.get().close();
                }
            }

            private void tryLoadNextAddFile() {
                if (nextAddFile.isPresent()) return;

                while (true) {
                    // Step 1: ensure `currBatchIter` has at least 1 more row. iterate through
                    //         `batchesIter` if necessary
                    while (!currBatchIter.isPresent() || !currBatchIter.get().hasNext()) {
                        if (currBatchIter.isPresent()) {
                            try {
                                currBatchIter.get().close();
                                currBatchIter = Optional.empty();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        if (!batchesIter.hasNext()) return; // No more batches!

                        // even if this next `currBatchIter` doesn't have any rows, the while loop
                        // will check for it and keep searching
                        final DataReadResult dataReadResult = batchesIter.next();
                        assert(dataReadResult.getData().getSchema().equals(ADD_ONLY_DATA_SCHEMA));
                        currBatchIter = Optional.of(dataReadResult.getData().getRows());
                        currBatchSelectionVector = dataReadResult.getSelectionVector();
                        currBatchRowIndex = -1;
                    }

                    // Step 2: now, `currBatchIter` has at least 1 more row to read. Read that row
                    //         and return if it passes the selection vector. If not, return to top.
                    currBatchRowIndex++;
                    final Row row = currBatchIter.get().next();
                    if (!currBatchSelectionVector.isPresent() ||
                          currBatchSelectionVector.get().getBoolean(currBatchRowIndex)) {
                        final Row addFileRowAtColumn0 = row.getStruct(0);
                        nextAddFile = Optional.of(
                            AddFile
                                .fromRow(addFileRowAtColumn0)
                                .withAbsolutePath(dataPath)
                        );

                        return;
                    }
                }
            }
        };
    }

    ////////////////////
    // Helper Methods //
    ////////////////////

    private Tuple2<Protocol, Metadata> loadTableProtocolAndMetadata() {
        Protocol protocol = null;
        Metadata metadata = null;

        try (CloseableIterator<Tuple2<FileDataReadResult, Boolean>> reverseIter =
                 new ActionsIterator(
                     tableClient,
                     logSegment.allLogFilesReversed(),
                     PROTOCOL_METADATA_READ_SCHEMA)) {
            while (reverseIter.hasNext()) {
                final ColumnarBatch columnarBatch = reverseIter.next()._1.getData();

                assert(columnarBatch.getSchema().equals(PROTOCOL_METADATA_READ_SCHEMA));

                if (protocol == null) {
                    final ColumnVector protocolVector = columnarBatch.getColumnVector(0);

                    for (int i = 0; i < protocolVector.getSize(); i++) {
                        if (!protocolVector.isNullAt(i)) {
                            final Row row = protocolVector.getStruct(i);
                            protocol = Protocol.fromRow(row);

                            if (metadata != null) {
                                // Stop since we have found the latest Protocol and Metadata.
                                validateSupportedTable(protocol, metadata);
                                return new Tuple2<>(protocol, metadata);
                            }

                            break; // We already found the protocol, exit this for-loop
                        }
                    }
                }
                if (metadata == null) {
                    final ColumnVector metadataVector = columnarBatch.getColumnVector(1);

                    for (int i = 0; i < metadataVector.getSize(); i++) {
                        if (!metadataVector.isNullAt(i)) {
                            final Row row = metadataVector.getStruct(i);
                            metadata = Metadata.fromRow(row, tableClient);

                            if (protocol != null) {
                                // Stop since we have found the latest Protocol and Metadata.
                                validateSupportedTable(protocol, metadata);
                                return new Tuple2<>(protocol, metadata);
                            }

                            break; // We already found the metadata, exit this for-loop
                        }
                    }
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException("Could not close iterator", ex);
        }

        if (protocol == null) {
            throw new IllegalStateException(
                String.format("No protocol found at version %s", logSegment.version)
            );
        }

        throw new IllegalStateException(
            String.format("No metadata found at version %s", logSegment.version)
        );
    }

    private void validateSupportedTable(Protocol protocol, Metadata metadata) {
        switch (protocol.getMinReaderVersion()) {
            case 1:
                break;
            case 2:
                verifySupportedColumnMappingMode(metadata);
                break;
            case 3:
                List<String> readerFeatures = protocol.getReaderFeatures();
                for (String readerFeature : readerFeatures) {
                    switch (readerFeature) {
                        case "deletionVectors":
                            break;
                        case "columnMapping":
                            verifySupportedColumnMappingMode(metadata);
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                "Unsupported table feature: " + readerFeature);
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported protocol version: " + protocol.getMinReaderVersion());
        }
    }

    private void verifySupportedColumnMappingMode(Metadata metadata) {
        // Check if the mode is name. Id mode is not yet supported
        String cmMode = metadata.getConfiguration().get("delta.columnMapping.mode");
        if (!"none".equalsIgnoreCase(cmMode) &&
            !"name".equalsIgnoreCase(cmMode)) {
            throw new UnsupportedOperationException(
                "Unsupported column mapping mode: " + cmMode);
        }
    }

    /**
     * Verifies that a set of delta or checkpoint files to be read actually belongs to this table.
     */
    private static void assertLogFilesBelongToTable(Path logPath, List<FileStatus> allFiles) {
        // TODO:
    }
}
