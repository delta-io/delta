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
package io.delta.kernel.internal.checkpoints;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import io.delta.kernel.client.FileReadContext;
import io.delta.kernel.client.JsonHandler;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.FileDataReadResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.fs.FileStatus;

import io.delta.kernel.internal.fs.Path;

import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Utils;

import io.delta.kernel.internal.util.InternalUtils;

/**
 * Class to load the {@link CheckpointMetaData} from `_last_checkpoint` file.
 */
public class Checkpointer
{
    /**
     * The name of the last checkpoint file
     */
    public static final String LAST_CHECKPOINT_FILE_NAME = "_last_checkpoint";

    /**
     * Given a list of checkpoint files, pick the latest complete checkpoint instance which is not
     * later than `notLaterThan`.
     */
    public static Optional<CheckpointInstance> getLatestCompleteCheckpointFromList(
        List<CheckpointInstance> instances,
        CheckpointInstance notLaterThan)
    {
        final List<CheckpointInstance> completeCheckpoints = instances
            .stream()
            .filter(c -> c.isNotLaterThan(notLaterThan))
            .collect(Collectors.groupingBy(c -> c))
            .entrySet()
            .stream()
            .filter(entry -> {
                final CheckpointInstance key = entry.getKey();
                final List<CheckpointInstance> inst = entry.getValue();

                if (key.numParts.isPresent()) {
                    return inst.size() == entry.getKey().numParts.get();
                }
                else {
                    return inst.size() == 1;
                }
            })
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

        if (completeCheckpoints.isEmpty()) {
            return Optional.empty();
        }
        else {
            return Optional.of(Collections.max(completeCheckpoints));
        }
    }

    /**
     * The path to the file that holds metadata about the most recent checkpoint.
     */
    private final Path lastCheckpointFilePath;

    public Checkpointer(Path tableLogPath)
    {
        this.lastCheckpointFilePath = new Path(tableLogPath, LAST_CHECKPOINT_FILE_NAME);
    }

    /**
     * Returns information about the most recent checkpoint.
     */
    public Optional<CheckpointMetaData> readLastCheckpointFile(TableClient tableClient)
    {
        return loadMetadataFromFile(tableClient);
    }

    /**
     * Loads the checkpoint metadata from the _last_checkpoint file.
     */
    private Optional<CheckpointMetaData> loadMetadataFromFile(TableClient tableClient)
    {
        try {
            // TODO: we have no way to get the file size and modification time within the api
            // module. Should we have a client API for that or make use of the
            // `FileSystemClient#listFrom`?
            FileStatus lastCheckpointFile = FileStatus.of(lastCheckpointFilePath.toString(), 0, 0);
            JsonHandler jsonHandler = tableClient.getJsonHandler();
            try (CloseableIterator<FileReadContext> fileReadContextIter =
                jsonHandler.contextualizeFileReads(
                    Utils.singletonCloseableIterator(
                        InternalUtils.getScanFileRow(lastCheckpointFile)),
                    Literal.TRUE
                );
                CloseableIterator<FileDataReadResult> jsonIter =
                    tableClient.getJsonHandler().readJsonFiles(
                        fileReadContextIter,
                        CheckpointMetaData.READ_SCHEMA)) {

                Optional<Row> checkpointRow = InternalUtils.getSingularRow(jsonIter);
                return checkpointRow.map(row -> CheckpointMetaData.fromRow(row));
            }
        }
        catch (Exception ex) {
            return Optional.empty();
        }
    }
}
