/*
 *  Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.flink.sink;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class DelayFinishTestSourceV2<T>
    implements Source<T, DelayFinishTestSourceV2.DFTSplit, Void> {

  @Override
  public Boundedness getBoundedness() {
    return null;
  }

  @Override
  public SplitEnumerator<DFTSplit, Void> createEnumerator(
      SplitEnumeratorContext<DFTSplit> enumContext) throws Exception {
    return null;
  }

  @Override
  public SplitEnumerator<DFTSplit, Void> restoreEnumerator(
      SplitEnumeratorContext<DFTSplit> enumContext, Void checkpoint) throws Exception {
    return null;
  }

  @Override
  public SimpleVersionedSerializer<DFTSplit> getSplitSerializer() {
    return null;
  }

  @Override
  public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
    return null;
  }

  @Override
  public SourceReader<T, DFTSplit> createReader(SourceReaderContext readerContext)
      throws Exception {
    return null;
  }

  public static class DFTSplit implements SourceSplit {
    private final String splitId;

    public DFTSplit(String id) {
      this.splitId = id;
    }

    @Override
    public String splitId() {
      return splitId;
    }
  }

  public static class DFTSplitEnumerator implements SplitEnumerator<DFTSplit, Void> {
    private final SplitEnumeratorContext<DFTSplit> context;
    private final Set<Integer> assigned = new HashSet<>();

    public DFTSplitEnumerator(SplitEnumeratorContext<DFTSplit> context) {
      this.context = context;
    }

    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, String hostname) {
      if (!assigned.contains(subtaskId)) {
        assigned.add(subtaskId);
        context.assignSplit(new DFTSplit("split-" + subtaskId), subtaskId);
        context.signalNoMoreSplits(subtaskId);
      }
    }

    @Override
    public void addSplitsBack(List<DFTSplit> splits, int subtaskId) {
      assigned.remove(subtaskId);
    }

    @Override
    public void addReader(int subtaskId) {}

    @Override
    public Void snapshotState(long checkpointId) throws Exception {
      return null; // no state
    }

    @Override
    public void close() throws IOException {}
  }

  public static class DFTReader<T> implements SourceReader<T, DFTSplit> {

    private final SourceReaderContext context;
    private boolean exhausted = false;
    private List<T> data;
    private Iterator<T> iterator = null;

    private AtomicInteger counter = new AtomicInteger(2);

    public DFTReader(List<T> data, SourceReaderContext context) {
      this.data = data;
      this.context = context;
      this.iterator = data.iterator();
    }

    @Override
    public void start() {}

    @Override
    public InputStatus pollNext(ReaderOutput<T> out) throws Exception {
      if (exhausted) {
        // When the data exhausts, wait for checkpoints
        while (counter.get() > 0) {
          Thread.sleep(100L);
          return InputStatus.MORE_AVAILABLE;
        }
        return InputStatus.END_OF_INPUT;
      }

      if (iterator.hasNext()) {
        T value = iterator.next();
        out.collect(value);
        return InputStatus.MORE_AVAILABLE;
      } else {
        exhausted = true;
        return InputStatus.MORE_AVAILABLE;
      }
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
      return null;
    }

    @Override
    public List<DFTSplit> snapshotState(long checkpointId) {
      return List.of();
    }

    @Override
    public void addSplits(List<DFTSplit> splits) {
      // You can ignore split contents if simple, or store them for use
    }

    @Override
    public void notifyNoMoreSplits() {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
      counter.decrementAndGet();
    }

    @Override
    public void close() throws IOException {}
  }
}
