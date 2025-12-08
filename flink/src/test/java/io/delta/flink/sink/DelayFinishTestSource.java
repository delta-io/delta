/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

/**
 * A bounded FLIP-27 (V2) Source that emits a fixed list of elements provided via constructor.
 *
 * <p>Splitting strategy:
 *
 * <ul>
 *   <li>Enumerator partitions the input list into N contiguous ranges, one per reader subtask.
 *   <li>Each reader emits its range and then finishes.
 * </ul>
 *
 * <p>State:
 *
 * <ul>
 *   <li>Split state = current index within that range (for recovery).
 *   <li>Enumerator checkpoint state = list of unassigned splits.
 * </ul>
 *
 * <p>Note: The elements must be Java-serializable because this example serializes them for splits.
 * In real connectors, splits typically contain references (paths, offsets), not the data itself.
 */
public class DelayFinishTestSource<T extends Serializable>
    implements Source<
        T, DelayFinishTestSource.ListSplit<T>, DelayFinishTestSource.EnumeratorState<T>> {

  private final List<T> elements;
  private final int delay;

  public DelayFinishTestSource(List<T> elements, int delay) {
    this.elements = List.copyOf(Preconditions.checkNotNull(elements, "elements"));
    this.delay = delay;
  }

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.BOUNDED;
  }

  @Override
  public SourceReader<T, ListSplit<T>> createReader(SourceReaderContext readerContext) {
    return new ListSplitReader<>(delay, readerContext);
  }

  @Override
  public SplitEnumerator<ListSplit<T>, EnumeratorState<T>> createEnumerator(
      SplitEnumeratorContext<ListSplit<T>> enumContext) {
    return new ListSplitEnumerator<>(enumContext, elements, /* restoredState */ null);
  }

  @Override
  public SplitEnumerator<ListSplit<T>, EnumeratorState<T>> restoreEnumerator(
      SplitEnumeratorContext<ListSplit<T>> enumContext, EnumeratorState<T> checkpoint) {
    return new ListSplitEnumerator<>(enumContext, elements, checkpoint);
  }

  @Override
  public SimpleVersionedSerializer<ListSplit<T>> getSplitSerializer() {
    return new ListSplitSerializer<>();
  }

  @Override
  public SimpleVersionedSerializer<EnumeratorState<T>> getEnumeratorCheckpointSerializer() {
    return new EnumeratorStateSerializer<>();
  }

  // -------------------------
  // Split
  // -------------------------

  /**
   * A split contains the elements for that split (data-carrying split, for demo purposes) and a
   * cursor index for resuming.
   */
  public static final class ListSplit<T extends Serializable> implements SourceSplit, Serializable {
    private final String splitId;
    private final List<T> data;
    private final int startInclusive; // for info/debug
    private final int endExclusive; // for info/debug

    public ListSplit(String splitId, List<T> data, int startInclusive, int endExclusive) {
      this.splitId = splitId;
      this.data = List.copyOf(data);
      this.startInclusive = startInclusive;
      this.endExclusive = endExclusive;
    }

    @Override
    public String splitId() {
      return splitId;
    }

    public List<T> data() {
      return data;
    }

    public int startInclusive() {
      return startInclusive;
    }

    public int endExclusive() {
      return endExclusive;
    }
  }

  // -------------------------
  // Enumerator state
  // -------------------------

  /** Checkpoint state: splits not yet assigned (or re-assignable on recovery). */
  public static final class EnumeratorState<T extends Serializable> implements Serializable {
    private final List<ListSplit<T>> remainingSplits;

    public EnumeratorState(List<ListSplit<T>> remainingSplits) {
      this.remainingSplits = List.copyOf(remainingSplits);
    }

    public List<ListSplit<T>> remainingSplits() {
      return remainingSplits;
    }
  }

  // -------------------------
  // Enumerator
  // -------------------------

  private static final class ListSplitEnumerator<T extends Serializable>
      implements SplitEnumerator<ListSplit<T>, EnumeratorState<T>> {

    private final SplitEnumeratorContext<ListSplit<T>> context;
    private final Deque<ListSplit<T>> unassigned;

    ListSplitEnumerator(
        SplitEnumeratorContext<ListSplit<T>> context,
        List<T> allElements,
        @Nullable EnumeratorState<T> restoredState) {

      this.context = context;

      if (restoredState != null) {
        this.unassigned = new ArrayDeque<>(restoredState.remainingSplits());
      } else {
        this.unassigned = new ArrayDeque<>(createSplits(allElements, context.currentParallelism()));
      }
    }

    @Override
    public void start() {
      // no-op; we assign on reader registration / split request.
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
      assignNextTo(subtaskId);
    }

    @Override
    public void addSplitsBack(List<ListSplit<T>> splits, int subtaskId) {
      // On failure/restart, Flink may return splits; put them back to be reassigned.
      for (ListSplit<T> s : splits) {
        unassigned.addFirst(s);
      }
    }

    @Override
    public void addReader(int subtaskId) {
      // Proactively assign when a reader registers.
      assignNextTo(subtaskId);
    }

    @Override
    public EnumeratorState<T> snapshotState(long checkpointId) {
      return new EnumeratorState<>(new ArrayList<>(unassigned));
    }

    @Override
    public void close() {
      // no-op
    }

    private void assignNextTo(int subtaskId) {
      // In this example, each subtask should get exactly one split (its partition).
      // If already assigned or none left, do nothing.
      if (unassigned.isEmpty()) {
        // Signal end-of-input once all splits are assigned and completed.
        // Flink considers bounded sources finished when readers return END_OF_INPUT;
        // we don't need to notify here.
        return;
      }

      // Assign splits that "belong" to the subtask if possible; otherwise just pop one.
      // (Belonging is encoded in splitId "split-{subtaskId}".)
      ListSplit<T> match = null;
      for (ListSplit<T> s : unassigned) {
        if (s.splitId().equals("split-" + subtaskId)) {
          match = s;
          break;
        }
      }
      final ListSplit<T> split = (match != null) ? match : unassigned.peekFirst();
      unassigned.remove(split);

      context.assignSplits(
          new SplitsAssignment<>(
              Collections.singletonMap(subtaskId, Collections.singletonList(split))));
    }

    private static <T extends Serializable> List<ListSplit<T>> createSplits(
        List<T> elems, int parallelism) {
      if (elems.isEmpty()) {
        // still create empty splits so each reader can end cleanly
        return java.util.stream.IntStream.range(0, parallelism)
            .mapToObj(i -> new ListSplit<T>("split-" + i, List.of(), 0, 0))
            .collect(Collectors.toList());
      }

      int p = Math.max(1, parallelism);
      int n = elems.size();

      List<ListSplit<T>> splits = new ArrayList<>(p);
      for (int subtask = 0; subtask < p; subtask++) {
        int start = (int) (((long) subtask * n) / p);
        int end = (int) (((long) (subtask + 1) * n) / p);
        List<T> slice = elems.subList(start, end);
        splits.add(new ListSplit<>("split-" + subtask, slice, start, end));
      }
      return splits;
    }
  }

  // -------------------------
  // Reader
  // -------------------------

  private static final class ListSplitReader<T extends Serializable>
      implements SourceReader<T, ListSplit<T>> {

    private final SourceReaderContext context;

    private @Nullable ListSplit<T> split;
    private int index;
    private volatile int delay;
    private boolean exhaust = false;

    ListSplitReader(int delay, SourceReaderContext context) {
      this.delay = delay;
      this.context = context;
      this.index = 0;
    }

    @Override
    public void start() {
      // no-op
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) {
      if (split == null) {
        return InputStatus.NOTHING_AVAILABLE;
      }
      if (index >= split.data().size()) {
        if (delay <= 0) {
          return InputStatus.END_OF_INPUT; // bounded: we're done
        } else {
          exhaust = true;
          return InputStatus.NOTHING_AVAILABLE;
        }
      }
      output.collect(split.data().get(index));
      index++;
      return InputStatus.MORE_AVAILABLE;
    }

    @Override
    public List<ListSplit<T>> snapshotState(long checkpointId) {
      // We encode progress by trimming already-emitted prefix into a new split.
      // This keeps it simple without a separate split-state class.
      if (split == null) {
        return List.of();
      }
      List<T> remaining =
          split.data().subList(Math.min(index, split.data().size()), split.data().size());
      return List.of(
          new ListSplit<>(
              split.splitId(), remaining, split.startInclusive() + index, split.endExclusive()));
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
      return new CompletableFuture<>() {
        @Override
        public boolean isDone() {
          return true;
        }
      };
    }

    @Override
    public void addSplits(List<ListSplit<T>> splits) {
      // This example expects one split.
      if (!splits.isEmpty()) {
        this.split = splits.get(0);
        this.index = 0;
      }
    }

    @Override
    public void notifyNoMoreSplits() {
      // no-op; we terminate when split is exhausted.
    }

    @Override
    public void close() {
      // no-op
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
      // no-op
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      // no-op
      if (exhaust) {
        delay--;
      }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
      // no-op
    }

    @Override
    public void pauseOrResumeSplits(
        Collection<String> splitsToPause, Collection<String> splitsToResume) {
      // bounded + single split => ignore for this minimal example
    }
  }

  // -------------------------
  // Serializers
  // -------------------------

  /** Serializer for ListSplit using Java serialization (demo). */
  private static final class ListSplitSerializer<T extends Serializable>
      implements SimpleVersionedSerializer<ListSplit<T>> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
      return VERSION;
    }

    @Override
    public byte[] serialize(ListSplit<T> split) throws IOException {
      return javaSerialize(split);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ListSplit<T> deserialize(int version, byte[] serialized) throws IOException {
      Preconditions.checkArgument(version == VERSION, "Unknown version: %s", version);
      return (ListSplit<T>) javaDeserialize(serialized);
    }
  }

  /** Serializer for EnumeratorState using Java serialization (demo). */
  private static final class EnumeratorStateSerializer<T extends Serializable>
      implements SimpleVersionedSerializer<EnumeratorState<T>> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
      return VERSION;
    }

    @Override
    public byte[] serialize(EnumeratorState<T> state) throws IOException {
      return javaSerialize(state);
    }

    @SuppressWarnings("unchecked")
    @Override
    public EnumeratorState<T> deserialize(int version, byte[] serialized) throws IOException {
      Preconditions.checkArgument(version == VERSION, "Unknown version: %s", version);
      return (EnumeratorState<T>) javaDeserialize(serialized);
    }
  }

  private static byte[] javaSerialize(Object obj) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(obj);
    }
    return baos.toByteArray();
  }

  private static Object javaDeserialize(byte[] bytes) throws IOException {
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      return ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed to deserialize", e);
    }
  }
}
