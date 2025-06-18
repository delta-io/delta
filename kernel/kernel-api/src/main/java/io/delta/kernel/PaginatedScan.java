package io.delta.kernel;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.replay.LogReplayUtils;
import io.delta.kernel.utils.CloseableIterator;

import java.util.HashSet;
import java.util.Set;

public interface PaginatedScan extends Scan {
    // maybe a row, not string
    String getNewPageTokenString();

    @Override
    CloseableIterator<FilteredColumnarBatch> getScanFiles(Engine engine);

    // return set (not hash-set)
    Set<LogReplayUtils.UniqueFileActionTuple> getTombStoneFromJson();
    Set<LogReplayUtils.UniqueFileActionTuple> getAddFilesFromJson();
}