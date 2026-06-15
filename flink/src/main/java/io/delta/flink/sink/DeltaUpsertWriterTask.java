/*
 *  Copyright (2026) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.sink;

import io.delta.flink.sink.mergestrategy.Upsert;
import io.delta.flink.table.DeltaTable;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;

/**
 * A {@link DeltaWriterTask} that supports update and delete by primary key.
 *
 * <p>Within a checkpoint the task tracks every PK it has seen so a repeat write for the same PK
 * overwrites the in-memory row in place, and a write or delete for a PK already flushed to a
 * Parquet file earlier in this checkpoint queues that row's position for deletion-vector scrubbing.
 * {@link #complete()} stamps those DVs onto the per-file {@code AddFile} actions.
 */
public class DeltaUpsertWriterTask extends DeltaWriterTask {

  /** PK hash &rarr; current index into {@link #buffer} for rows still live in memory. */
  private final Map<Integer, Integer> keyToPositionInMemory;

  /** {@code true} when {@link #buffer} contains {@code null} holes left by {@link #erase}. */
  private boolean sparseMemory = false;

  /**
   * One entry per dumped Parquet file (in dump order): PK hash &rarr; row index in that file. Used
   * by {@link #markAsRemoved} to locate rows produced earlier in the checkpoint.
   */
  private final List<Map<Integer, Integer>> keyToPositionInFiles;

  /** Parallel to {@link #keyToPositionInFiles}: row indices to scrub via DV at complete time. */
  private final List<Set<Integer>> positionsToRemove;

  private final Upsert algorithm;

  public DeltaUpsertWriterTask(
      String jobId,
      int subtaskId,
      int attemptNumber,
      DeltaTable deltaTable,
      DeltaSinkConf conf,
      Map<String, Literal> partitionValues,
      Upsert algorithm) {
    super(jobId, subtaskId, attemptNumber, deltaTable, conf, partitionValues);
    keyToPositionInMemory = new HashMap<>();
    keyToPositionInFiles = new ArrayList<>();
    positionsToRemove = new ArrayList<>();
    this.algorithm = algorithm;
  }

  /**
   * Records {@code element} under {@code primaryKey}. If a row for this PK already lives in the
   * buffer it is replaced in place; if it lives only in an already-dumped file the new row is
   * appended and the old one is queued for DV scrubbing.
   *
   * @return {@code true} if a prior row for {@code primaryKey} was found (in memory or on disk)
   */
  public boolean write(List<Literal> primaryKey, RowData element, SinkWriter.Context context)
      throws IOException, InterruptedException {
    int keyHash = MergeStrategy.keyHash(primaryKey);
    int index = markAsRemoved(keyHash);
    if (index >= 0) {
      buffer.set(index, element);
      highWatermark = Math.max(highWatermark, context.currentWatermark());
      lowWatermark = Math.min(lowWatermark, context.currentWatermark());
    } else {
      keyToPositionInMemory.put(keyHash, buffer.size());
      super.write(element, context);
    }
    return index != -1;
  }

  /**
   * Removes any row in this checkpoint matching {@code primaryKey}. An in-memory row is nulled out
   * (compacted on the next dump); a row already on disk is queued for DV scrubbing in {@link
   * #complete()}.
   *
   * @return {@code true} if a prior row was found and marked for removal
   */
  public boolean erase(List<Literal> primaryKey) {
    int removeIndex = markAsRemoved(MergeStrategy.keyHash(primaryKey));
    sparseMemory |= removeIndex >= 0;
    return removeIndex != -1;
  }

  /**
   * Compacts the buffer, snapshots the surviving PK-to-row-position map so later operations in the
   * same checkpoint can still locate rows in this file, then delegates to {@link
   * DeltaWriterTask#dump()} to write the Parquet file. No-op when nothing remains to flush.
   */
  @Override
  protected void dump() throws IOException {
    compressInMemoryBuffer();
    if (keyToPositionInMemory.isEmpty()) {
      return;
    }
    keyToPositionInFiles.add(Map.copyOf(keyToPositionInMemory));
    positionsToRemove.add(new HashSet<>());
    super.dump();
    keyToPositionInMemory.clear();
  }

  /**
   * Flushes the remaining buffer and decorates each {@link DeltaWriterResult} with a deletion
   * vector covering the row positions that later writes in this checkpoint superseded; results
   * without queued removals are returned unchanged.
   */
  @Override
  public List<DeltaWriterResult> complete() throws IOException {
    List<DeltaWriterResult> results = super.complete();
    // Make sure the number of AddFiles is the same as recorded indices
    // Require each dump to generate exactly one AddFile for now.
    Validate.isTrue(results.size() == positionsToRemove.size());

    // Map every (writerResult, positionsToRemove) pair to a freshly-built DeltaWriterResult.
    // Neither the input `results` list nor the original DeltaWriterResults are mutated:
    // when there's no DV to add we return the original reference unchanged; when there is,
    // we allocate a new result that shares only the immutable WriterResultContext.

    return IntStream.range(0, results.size())
        .<Optional<DeltaWriterResult>>mapToObj(
            i -> {
              DeltaWriterResult result = results.get(i);
              Set<Integer> positions = positionsToRemove.get(i);
              if (positions.isEmpty()) {
                return Optional.of(result);
              }
              if (positions.size() == keyToPositionInFiles.get(i).size()) {
                return Optional.empty();
              }
              Row stagedAction = result.getDeltaActions().get(0);
              Row marked = algorithm.markRemovesOnStaged(deltaTable, stagedAction, positions);
              return Optional.of(new DeltaWriterResult(List.of(marked), result.getContext()));
            })
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  /**
   * Marks the row for {@code keyHash} as removed: nulls its slot in {@link #buffer} if it is still
   * in memory, or adds its position to {@link #positionsToRemove} if it has already been dumped to
   * a Parquet file earlier in this checkpoint.
   *
   * @return the in-memory slot index ({@code >= 0}); {@code -2} if the row was on disk; {@code -1}
   *     if no row for {@code keyHash} was tracked
   */
  protected int markAsRemoved(int keyHash) {
    int existIndex = keyToPositionInMemory.getOrDefault(keyHash, -1);
    if (existIndex >= 0) {
      buffer.set(existIndex, null);
    } else {
      for (int i = 0; i < keyToPositionInFiles.size(); i++) {
        int index = keyToPositionInFiles.get(i).getOrDefault(keyHash, -1);
        if (index >= 0 && !positionsToRemove.get(i).contains(index)) {
          positionsToRemove.get(i).add(index);
          existIndex = -2;
          break;
        }
      }
    }
    return existIndex;
  }

  /**
   * Two-pointer pass that removes the {@code null} holes left by {@link #erase} from {@link
   * #buffer} and rebuilds {@link #keyToPositionInMemory} with the compacted indices. No-op when the
   * buffer is known to be dense.
   */
  protected void compressInMemoryBuffer() {
    if (!sparseMemory) {
      return;
    }
    int writeIndex = 0;
    int[] keyToPositionArray = new int[buffer.size()];
    keyToPositionInMemory.forEach((key, value) -> keyToPositionArray[value] = key);
    keyToPositionInMemory.clear();
    for (int readIndex = 0; readIndex < buffer.size(); readIndex++) {
      if (buffer.get(readIndex) != null) {
        if (writeIndex != readIndex) {
          buffer.set(writeIndex, buffer.get(readIndex));
        }
        keyToPositionInMemory.put(keyToPositionArray[readIndex], writeIndex);
        writeIndex++;
      }
    }
    buffer.subList(writeIndex, buffer.size()).clear();
    sparseMemory = false;
  }
}
