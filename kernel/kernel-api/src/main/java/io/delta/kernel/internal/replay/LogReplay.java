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
import java.util.Optional;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FileDataReadResult;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.actions.SetTransaction;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.util.Logging;
import io.delta.kernel.internal.util.Tuple2;

/**
 * Replays a history of actions, resolving them to produce the current state of the table. The
 * protocol for resolution is as follows:
 *  - The most recent {@code AddFile} and accompanying metadata for any `(path, dv id)` tuple wins.
 *  - {@code RemoveFile} deletes a corresponding AddFile. A {@code RemoveFile} "corresponds" to
 *    the AddFile that matches both the parquet file URI *and* the deletion vector's URI (if any).
 *  - The most recent {@code Metadata} wins.
 *  - The most recent {@code Protocol} version wins.
 *  - For each `(path, dv id)` tuple, this class should always output only one {@code FileAction}
 *    (either {@code AddFile} or {@code RemoveFile})
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

    public static final StructType SET_TRANSACTION_READ_SCHEMA = new StructType()
        .add("txn", SetTransaction.READ_SCHEMA);

    /**
     * Read schema when searching for all the active AddFiles (need some RemoveFile info, too).
     * Note that we don't need to read the entire RemoveFile, only the path and dv info.
     */
    public static final StructType ADD_REMOVE_READ_SCHEMA = new StructType()
        // TODO: further restrict the fields to read from AddFile depending upon
        // the whether stats are needed or not: https://github.com/delta-io/delta/issues/1961
        .add("add", AddFile.SCHEMA)
        .add("remove", new StructType()
            .add("path", StringType.STRING, false /* nullable */)
            .add("deletionVector", DeletionVectorDescriptor.READ_SCHEMA, true /* nullable */)
        );

    public static int ADD_FILE_ORDINAL = ADD_REMOVE_READ_SCHEMA.indexOf("add");
    public static int ADD_FILE_PATH_ORDINAL =
        ((StructType) ADD_REMOVE_READ_SCHEMA.get("add").getDataType()).indexOf("path");
    public static int ADD_FILE_DV_ORDINAL =
        ((StructType) ADD_REMOVE_READ_SCHEMA.get("add").getDataType()).indexOf("deletionVector");

    public static int REMOVE_FILE_ORDINAL = ADD_REMOVE_READ_SCHEMA.indexOf("remove");
    public static int REMOVE_FILE_PATH_ORDINAL =
        ((StructType) ADD_REMOVE_READ_SCHEMA.get("remove").getDataType()).indexOf("path");
    public static int REMOVE_FILE_DV_ORDINAL =
        ((StructType) ADD_REMOVE_READ_SCHEMA.get("remove").getDataType()).indexOf("deletionVector");

    /** Data (result) schema of the remaining active AddFiles. */
    public static final StructType ADD_ONLY_DATA_SCHEMA = new StructType()
        .add("add", AddFile.SCHEMA);

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

    public Optional<Long> loadRecentTransactionVersion(String applicationId) {
        // TODO: do we need to cache it? expected to call multiple times?
        return loadRecentTransactionVersionHelper(applicationId);
    }

    /**
     * Returns an iterator of {@link FilteredColumnarBatch} with schema
     * {@link #ADD_ONLY_DATA_SCHEMA} representing all the active AddFiles in the table
     */
    public CloseableIterator<FilteredColumnarBatch> getAddFilesAsColumnarBatches() {
        final CloseableIterator<Tuple2<FileDataReadResult, Boolean>> addRemoveIter =
            new ActionsIterator(
                tableClient,
                logSegment.allLogFilesReversed(),
                ADD_REMOVE_READ_SCHEMA);
        return new ActiveAddFilesIterator(tableClient, addRemoveIter, dataPath);
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
                            protocol = Protocol.fromColumnVector(protocolVector, i);

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
                            metadata = Metadata.fromColumnVector(metadataVector, i, tableClient);

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

    private Optional<Long> loadRecentTransactionVersionHelper(String applicationId) {
        try (CloseableIterator<Tuple2<FileDataReadResult, Boolean>> reverseIter =
                     new ActionsIterator(
                             tableClient,
                             logSegment.allLogFilesReversed(),
                             SET_TRANSACTION_READ_SCHEMA)) {
            while (reverseIter.hasNext()) {
                final ColumnarBatch columnarBatch = reverseIter.next()._1.getData();

                assert(columnarBatch.getSchema().equals(SET_TRANSACTION_READ_SCHEMA));
                final ColumnVector txnVector = columnarBatch.getColumnVector(0);
                for (int i = 0; i < txnVector.getSize(); i++) {
                    if (!txnVector.isNullAt(i)) {
                        SetTransaction txn = SetTransaction.fromColumnVector(txnVector, i);

                        if (txn != null && applicationId.equals(txn.getAppId())) {
                            return Optional.of(txn.getVersion());
                        }
                    }
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException("Could not close iterator", ex);
        }

        return Optional.empty();
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
        String cmMode = metadata.getConfiguration()
                .getOrDefault("delta.columnMapping.mode", "none");
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
