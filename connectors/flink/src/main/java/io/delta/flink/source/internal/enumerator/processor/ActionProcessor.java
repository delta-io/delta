package io.delta.flink.source.internal.enumerator.processor;

import java.util.ArrayList;
import java.util.List;

import io.delta.flink.source.internal.enumerator.monitor.ChangesPerVersion;
import io.delta.flink.source.internal.exceptions.DeltaSourceExceptions;
import static io.delta.flink.source.internal.exceptions.DeltaSourceExceptions.deltaSourceIgnoreChangesException;
import static io.delta.flink.source.internal.exceptions.DeltaSourceExceptions.deltaSourceIgnoreDeleteException;

import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;

/**
 * This class process {@link Action} from Delta table version and produces a collection of {@link
 * AddFile} object that were recorded for given version.
 */
public class ActionProcessor {

    /**
     * If set to true, allows for versions with {@link RemoveFile} only.
     */
    private final boolean ignoreDeletes;

    /**
     * If set to true, allows for Delta table versions with combination of {@link RemoveFile} and
     * {@link RemoveFile} actions. This field subsumes {@link #ignoreDeletes}
     */
    private final boolean ignoreChanges;

    public ActionProcessor(boolean ignoreChanges, boolean ignoreDeletes) {
        this.ignoreChanges = ignoreChanges;
        this.ignoreDeletes = ignoreDeletes || ignoreChanges;
    }

    /**
     * Process Delta table {@link Action} objects for given table version. Can throw an exception if
     * unsupported action was recorded such as {@link io.delta.standalone.actions.Metadata} or
     * {@link io.delta.standalone.actions.Protocol}.
     *
     * <p>
     * Additionally a sanity check is done for every input {@link ChangesPerVersion} to make sure
     * that contract version contains only allowed combination of {@link AddFile} and {@link
     * RemoveFile} actions. The result of this check depends on {@link #ignoreDeletes} and {@link
     * #ignoreChanges} fields.
     * <p>
     * This method can throw {@link io.delta.flink.source.internal.exceptions.DeltaSourceException}
     * if sanity check for version Actions fail.
     *
     * @param changesToProcess A {@link ChangesPerVersion} object containing all {@link Action}'s
     *                         for {@link ChangesPerVersion#getSnapshotVersion()}.
     * @return A {@link ChangesPerVersion} object containing a collection of {@link AddFile} for
     * input Delta table version.
     */
    public ChangesPerVersion<AddFile> processActions(ChangesPerVersion<Action> changesToProcess) {

        List<AddFile> addFiles = new ArrayList<>(changesToProcess.size());
        boolean seenAddFile = false;
        boolean seenRemovedFile = false;

        for (Action action : changesToProcess.getChanges()) {
            DeltaAction deltaAction = DeltaAction.instanceFrom(action.getClass());
            switch (deltaAction) {
                case ADD:
                    if (((AddFile) action).isDataChange()) {
                        seenAddFile = true;
                        addFiles.add((AddFile) action);
                    }
                    break;
                case REMOVE:
                    if (((RemoveFile) action).isDataChange()) {
                        seenRemovedFile = true;
                    }
                    break;
                case METADATA:
                case PROTOCOL:
                    throw DeltaSourceExceptions.unsupportedDeltaActionException(
                        changesToProcess.getDeltaTablePath(), changesToProcess.getSnapshotVersion(),
                        action);
                default:
                    // Inspired by https://github.com/delta-io/delta/blob/0d07d094ccd520c1adbe45dde4804c754c0a4baa/core/src/main/scala/org/apache/spark/sql/delta/sources/DeltaSource.scala#:~:text=case%20null%20%3D%3E%20//%20Some%20crazy%20future%20feature.%20Ignore
                    break;
            }
            actionsSanityCheck(seenAddFile, seenRemovedFile, changesToProcess);
        }

        return new ChangesPerVersion<>(
            changesToProcess.getDeltaTablePath(), changesToProcess.getSnapshotVersion(), addFiles);
    }

    /**
     * Performs a sanity check for processed version to verify if there were no invalid combination
     * of {@link RemoveFile} and {@link AddFile} actions.
     * <p>
     * Will throw a {@link io.delta.flink.source.internal.exceptions.DeltaSourceException} if check
     * fail.
     */
    private void actionsSanityCheck(boolean seenFileAdd, boolean seenRemovedFile,
        ChangesPerVersion<Action> changesToProcess) {
        if (seenRemovedFile) {
            if (seenFileAdd && !ignoreChanges) {
                throw deltaSourceIgnoreChangesException(
                    changesToProcess.getDeltaTablePath(), changesToProcess.getSnapshotVersion());
            } else if (!seenFileAdd && !ignoreDeletes) {
                throw deltaSourceIgnoreDeleteException(
                    changesToProcess.getDeltaTablePath(), changesToProcess.getSnapshotVersion());
            }
        }
    }
}
