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
import java.util.*;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.data.*;
import io.delta.kernel.utils.CloseableIterator;

import io.delta.kernel.internal.actions.SetTransaction;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.util.Utils;
import static io.delta.kernel.internal.actions.SingleAction.CHECKPOINT_SCHEMA;
import static io.delta.kernel.internal.replay.LogReplayUtils.*;
import static io.delta.kernel.internal.util.Preconditions.checkState;

/**
 * Replays a history of actions from the transaction log to reconstruct the checkpoint state of the
 * table. The rules for constructing the checkpoint state are defined in the Delta Protocol:
 * <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#action-reconciliation">
 * Checkpoint Reconciliation Rules</a>.
 * <p>
 * Currently, the following rules are implemented:
 * <ul>
 *     <li> The latest protocol action seen wins </li>
 *     <li> The latest metaData action seen wins </li>
 *     <li> For txn actions, the latest version seen for a given appId wins </li>
 *     <li> Logical files in a table are identified by their (path, deletionVector.uniqueId)
 *     primary key. File actions (add or remove) reference logical files, and a log can contain
 *     any number of references to a single file.</li>
 *     <li>To replay the log, scan all file actions and keep only the newest reference for each
 *     logical file.</li>
 *     <li>add actions in the result identify logical files currently present in the table
 *     (for queries). remove actions in the result identify tombstones of logical files no
 *     longer present in the table (for VACUUM).</li>
 *     <li>commit info actions are not included</li>
 * </ul>
 * <p>
 * Following rules are not implemented. They will be implemented as we add support for more
 * table features over time.
 * <ul>
 *     <li> For domainMetadata, the latest domainMetadata seen for a given domain wins.</li>
 * </ul>
 */
public class CreateCheckpointIterator implements CloseableIterator<FilteredColumnarBatch> {

    private static final int[] ADD_ORDINAL = getPathOrdinals(CHECKPOINT_SCHEMA, "add");
    private static final int[] ADD_PATH_ORDINAL =
            getPathOrdinals(CHECKPOINT_SCHEMA, "add", "path");
    private static final int[] ADD_DV_ORDINAL =
            getPathOrdinals(CHECKPOINT_SCHEMA, "add", "deletionVector");

    private static final int[] REMOVE_ORDINAL = getPathOrdinals(CHECKPOINT_SCHEMA, "remove");
    private static final int[] REMOVE_PATH_ORDINAL =
            getPathOrdinals(CHECKPOINT_SCHEMA, "remove", "path");
    private static final int[] REMOVE_DV_ORDINAL =
            getPathOrdinals(CHECKPOINT_SCHEMA, "remove", "deletionVector");
    private static final int[] REMOVE_DELETE_TIMESTAMP_ORDINAL =
            getPathOrdinals(CHECKPOINT_SCHEMA, "remove", "deletionTimestamp");

    private static final int[] PROTOCOL_ORDINAL = getPathOrdinals(CHECKPOINT_SCHEMA, "protocol");
    private static final int[] METADATA_ORDINAL = getPathOrdinals(CHECKPOINT_SCHEMA, "metaData");
    private static final int[] TXN_ORDINAL = getPathOrdinals(CHECKPOINT_SCHEMA, "txn");

    private final Engine engine;
    private final LogSegment logSegment;

    /**
     * Tombstones (i.e. RemoveFile) will be still kept in checkpoint until the tombstone timestamp
     * is earlier than this retention timestamp.
     */
    private final long minFileRetentionTimestampMillis;

    // State of the iterator and current batch being worked on
    private CloseableIterator<ActionWrapper> actionsIter;
    private boolean closed;
    private Optional<FilteredColumnarBatch> toReturnNext = Optional.empty();
    /**
     * This buffer is reused across batches to keep the memory allocations minimal. It is resized as
     * required and the array entries are reset between batches.
     */
    private boolean[] selectionVectorBuffer;

    // Current state of the tombstones and add files from delta files
    private final Set<UniqueFileActionTuple> tombstonesFromJson = new HashSet<>();
    private final Set<UniqueFileActionTuple> addFilesFromJson = new HashSet<>();

    // Current state of the protocol and metadata. Captures whether protocol or metadata is seen.
    // We traverse the log in reverse, so the first encounter of protocol or metadata is considered
    // latest.
    private boolean isMetadataAlreadySeen;
    private boolean isProtocolAlreadySeen;

    // Current state of the transaction identifier (a.k.a. SetTransaction). We traverse the log in
    // reverse, so storing the first seen transaction version for each appId is enough for
    // checkpoint
    private final Map<String, Long> txnAppIdToVersion = new HashMap<>();

    // Metadata about the checkpoint to store in `_last_checkpoint` file
    private long numberOfAddActions = 0; // final number of add actions survived in the checkpoint

    /////////////////
    // Public APIs //
    /////////////////

    public CreateCheckpointIterator(
            Engine engine,
            LogSegment logSegment,
            long minFileRetentionTimestampMillis) {
        this.engine = engine;
        this.logSegment = logSegment;
        this.minFileRetentionTimestampMillis = minFileRetentionTimestampMillis;
    }

    @Override
    public boolean hasNext() {
        initActionIterIfRequired();
        checkState(!closed, "Can't call `hasNext` on a closed iterator.");
        return prepareNext();
    }

    @Override
    public FilteredColumnarBatch next() {
        checkState(!closed, "Can't call `next` on a closed iterator.");
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        FilteredColumnarBatch toReturn = toReturnNext.get();
        toReturnNext = Optional.empty();
        return toReturn;
    }

    @Override
    public void close() throws IOException {
        closed = true;
        Utils.closeCloseables(actionsIter);
    }

    /**
     * Number of add files in the final checkpoint. Should be called once the entire data of this
     * iterator is consumed.
     *
     * @return Number of add files in checkpoint.
     */
    public long getNumberOfAddActions() {
        checkState(closed, "Iterator is not fully consumed yet.");
        return numberOfAddActions;
    }

    ////////////////////////////
    // Private Helper Methods //
    ////////////////////////////

    private void initActionIterIfRequired() {
        if (this.actionsIter == null) {
            this.actionsIter = new ActionsIterator(engine,
                    logSegment.allLogFilesReversed(),
                    CHECKPOINT_SCHEMA,
                    Optional.empty() /* checkpoint predicate */);
        }
    }

    /**
     * Prepare the next batch to return and store it in {@link #toReturnNext}
     *
     * @return true if there is data to return, false otherwise.
     */
    private boolean prepareNext() {
        if (toReturnNext.isPresent()) {
            return true;
        }
        if (!actionsIter.hasNext()) {
            return false;
        }

        ActionWrapper actionWrapper = actionsIter.next();
        final ColumnarBatch actionsBatch = actionWrapper.getColumnarBatch();
        final boolean isFromCheckpoint = actionWrapper.isFromCheckpoint();

        // Prepare the selection vector to attach to the batch to indicate which records to
        // write to checkpoint and which one or not
        selectionVectorBuffer =
                prepareSelectionVectorBuffer(selectionVectorBuffer, actionsBatch.getSize());

        // Step 1: Update `tombstonesFromJson` with all the RemoveFiles in this columnar batch, if
        // and only if this batch is not from a checkpoint. There's no reason to put a RemoveFile
        // from a checkpoint into `tombstonesFromJson` since, when we generate a checkpoint,
        // any corresponding AddFile would have been excluded already
        if (!isFromCheckpoint) {
            processRemoves(
                    getVector(actionsBatch, REMOVE_ORDINAL),
                    getVector(actionsBatch, REMOVE_PATH_ORDINAL),
                    getVector(actionsBatch, REMOVE_DV_ORDINAL),
                    getVector(actionsBatch, REMOVE_DELETE_TIMESTAMP_ORDINAL),
                    selectionVectorBuffer);
        }

        // Step 2: Iterate over all the AddFiles in this columnar batch in order to build up the
        //         selection vector. We unselect an AddFile when it was removed by a RemoveFile
        processAdds(
                getVector(actionsBatch, ADD_ORDINAL),
                getVector(actionsBatch, ADD_PATH_ORDINAL),
                getVector(actionsBatch, ADD_DV_ORDINAL),
                isFromCheckpoint,
                selectionVectorBuffer);

        // Step 3: Process the protocol
        final ColumnVector protocolVector = getVector(actionsBatch, PROTOCOL_ORDINAL);
        processProtocol(protocolVector, selectionVectorBuffer);

        // Step 3: Process the metadata
        final ColumnVector metadataVector = getVector(actionsBatch, METADATA_ORDINAL);
        processMetadata(metadataVector, selectionVectorBuffer);

        // Step 4: Process the transaction identifiers
        final ColumnVector txnVector = getVector(actionsBatch, TXN_ORDINAL);
        processTxn(txnVector, selectionVectorBuffer);

        Optional<ColumnVector> selectionVector =
                Optional.of(createSelectionVector(selectionVectorBuffer, actionsBatch.getSize()));
        toReturnNext = Optional.of(
                new FilteredColumnarBatch(actionsBatch, selectionVector));
        return true;
    }

    private void processRemoves(
            ColumnVector removesVector,
            ColumnVector removePathVector,
            ColumnVector removeDvVector,
            ColumnVector removeDeleteTimestampVector,
            boolean[] selectionVectorBuffer) {
        for (int rowId = 0; rowId < removesVector.getSize(); rowId++) {
            if (removesVector.isNullAt(rowId)) {
                continue; // selectionVector will be `false` at rowId by default
            }

            final UniqueFileActionTuple key =
                    getUniqueFileAction(removePathVector, removeDvVector, rowId);
            tombstonesFromJson.add(key);

            // Default is zero. Not sure if this the correct way, but it is same Delta Spark.
            // Ideally this should never be zero, but we are following the same behavior as Delta
            // Spark here.
            long deleteTimestamp = 0;
            if (!removeDeleteTimestampVector.isNullAt(rowId)) {
                deleteTimestamp = removeDeleteTimestampVector.getLong(rowId);
            }
            if (deleteTimestamp > minFileRetentionTimestampMillis) {
                // We still keep remove files in checkpoint as tombstones until the minimum
                // retention period has passed
                select(selectionVectorBuffer, rowId);
            }
        }
    }

    private void processAdds(
            ColumnVector addsVector,
            ColumnVector addPathVector,
            ColumnVector addDvVector,
            boolean isFromCheckpoint,
            boolean[] selectionVectorBuffer) {
        for (int rowId = 0; rowId < addsVector.getSize(); rowId++) {
            if (addsVector.isNullAt(rowId)) {
                continue; // selectionVector will be `false` at rowId by default
            }

            final UniqueFileActionTuple key =
                    getUniqueFileAction(addPathVector, addDvVector, rowId);
            final boolean alreadyDeleted = tombstonesFromJson.contains(key);
            final boolean alreadyReturned = addFilesFromJson.contains(key);

            if (!alreadyReturned) {
                // Note: No AddFile will appear twice in a checkpoint, so we only need
                //       non-checkpoint AddFiles in the set
                if (!isFromCheckpoint) {
                    addFilesFromJson.add(key);
                }

                if (!alreadyDeleted) {
                    numberOfAddActions++;
                    select(selectionVectorBuffer, rowId);
                }
            }
        }
    }

    private void processProtocol(ColumnVector protocolVector, boolean[] selectionVectorBuffer) {
        for (int rowId = 0; rowId < protocolVector.getSize(); rowId++) {
            if (protocolVector.isNullAt(rowId)) {
                continue; // selectionVector will be `false` at rowId by default
            }

            if (isProtocolAlreadySeen) {
                // We do a reverse log replay. The latest always the one that should be written
                // to the checkpoint. Anything after the first one shouldn't be in checkpoint
                unselect(selectionVectorBuffer, rowId);
            } else {
                select(selectionVectorBuffer, rowId);
                isProtocolAlreadySeen = true;
            }
        }
    }

    private void processMetadata(ColumnVector metadataVector, boolean[] selectionVectorBuffer) {
        for (int rowId = 0; rowId < metadataVector.getSize(); rowId++) {
            if (metadataVector.isNullAt(rowId)) {
                continue; // selectionVector will be `false` at rowId by default
            }

            if (isMetadataAlreadySeen) {
                // We do a reverse log replay. The latest always the one that should be written
                // to the checkpoint. Anything after the first one shouldn't be in checkpoint
                unselect(selectionVectorBuffer, rowId);
            } else {
                select(selectionVectorBuffer, rowId);
                isMetadataAlreadySeen = true;
            }
        }
    }

    private void processTxn(ColumnVector txnVector, boolean[] selectionVectorBuffer) {
        for (int rowId = 0; rowId < txnVector.getSize(); rowId++) {
            SetTransaction txn = SetTransaction.fromColumnVector(txnVector, rowId);
            if (txn == null) {
                continue; // selectionVector will be `false` at rowId by default
            }
            if (txnAppIdToVersion.containsKey(txn.getAppId())) {
                // We do a reverse log replay. The latest txn version is the one that should be
                // written to the checkpoint. Anything after the first one shouldn't be in
                // checkpoint
                unselect(selectionVectorBuffer, rowId);
            } else {
                select(selectionVectorBuffer, rowId);
                txnAppIdToVersion.put(txn.getAppId(), txn.getVersion());
            }
        }
    }

    private void unselect(boolean[] selectionVectorBuffer, int rowId) {
        // Just use the java assert (which are enabled in tests) for sanity checks. This should
        // never happen. Given this is going to be on the hot path, we want to avoid cost in
        // production.
        assert !selectionVectorBuffer[rowId] :
                "Row is already marked for selection, can't unselect now: " + rowId;
        selectionVectorBuffer[rowId] = false;
    }

    private void select(boolean[] selectionVectorBuffer, int rowId) {
        selectionVectorBuffer[rowId] = true;
    }

    private ColumnVector createSelectionVector(boolean[] selectionVectorBuffer, int size) {
        return engine.getExpressionHandler()
                .createSelectionVector(selectionVectorBuffer, 0, size);
    }
}
