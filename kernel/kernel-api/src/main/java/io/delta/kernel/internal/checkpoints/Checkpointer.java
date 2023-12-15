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

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.InternalUtils;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

/**
 * Class to load the {@link CheckpointMetaData} from `_last_checkpoint` file.
 */
public class Checkpointer {
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
        CheckpointInstance notLaterThan) {
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
                } else {
                    return inst.size() == 1;
                }
            })
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

        if (completeCheckpoints.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(Collections.max(completeCheckpoints));
        }
    }

    /**
     * The path to the file that holds metadata about the most recent checkpoint.
     */
    private final Path lastCheckpointFilePath;

    public Checkpointer(Path tableLogPath) {
        this.lastCheckpointFilePath = new Path(tableLogPath, LAST_CHECKPOINT_FILE_NAME);
    }

    /**
     * Returns information about the most recent checkpoint.
     */
    public Optional<CheckpointMetaData> readLastCheckpointFile(TableClient tableClient) {
        return loadMetadataFromFile(tableClient);
    }

    /**
     * Loads the checkpoint metadata from the _last_checkpoint file.
     */
    private Optional<CheckpointMetaData> loadMetadataFromFile(TableClient tableClient) {
        try {
            // For now we use file size = 0 and modification time = 0, in the future we should use
            // listFrom to retrieve the real values see delta-io/delta#2140
            FileStatus lastCheckpointFile = FileStatus.of(lastCheckpointFilePath.toString(), 0, 0);

            try(CloseableIterator<ColumnarBatch> jsonIter =
                tableClient.getJsonHandler().readJsonFiles(
                    singletonCloseableIterator(lastCheckpointFile),
                    CheckpointMetaData.READ_SCHEMA,
                    Optional.empty())) {
                Optional<Row> checkpointRow = InternalUtils.getSingularRow(jsonIter);
                return checkpointRow.map(CheckpointMetaData::fromRow);
            }
        } catch (Exception ex) {
            return Optional.empty();
        }
    }
}
