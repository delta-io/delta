package io.delta.flink.source.internal.enumerator;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

import javax.annotation.Nullable;

import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.source.internal.enumerator.DeltaSourceSplitEnumerator.AssignSplitStatus.NO_MORE_READERS;
import static io.delta.flink.source.internal.enumerator.DeltaSourceSplitEnumerator.AssignSplitStatus.NO_MORE_SPLITS;

/**
 * A base class for {@link SplitEnumerator} used by {@link io.delta.flink.source.DeltaSource}
 * <p>
 * The implementations that will choose to extend this class will have to implement abstract method
 * {@link DeltaSourceSplitEnumerator#handleNoMoreSplits(int)}
 */
public abstract class DeltaSourceSplitEnumerator implements
    SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>> {

    private static final Logger LOG =
        LoggerFactory.getLogger(DeltaSourceSplitEnumerator.class);

    /**
     * Path to Delta table that should be processed.
     */
    protected final Path deltaTablePath;

    /**
     * A {@link FileSplitAssigner} that should be used by this {@code SourceEnumerator}.
     */
    protected final FileSplitAssigner splitAssigner;

    /**
     * A {@link SplitEnumeratorContext} assigned to this {@code SourceEnumerator}.
     */
    protected final SplitEnumeratorContext<DeltaSourceSplit> enumContext;

    /**
     * Map containing all readers that have requested the split.
     * <p>
     * The key is the subtask id of the source reader who sent the source event. requesterHostname
     * <p>
     * The value is an optional hostname where the requesting task is running. This can be used to
     * make split assignments locality-aware.
     *
     * @implNote The type contract for this map comes from {@link #handleSplitRequest(int, String)}
     * method.
     */
    protected final LinkedHashMap<Integer, String> readersAwaitingSplit;


    protected DeltaSourceSplitEnumerator(
        Path deltaTablePath, FileSplitAssigner splitAssigner,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext) {

        this.deltaTablePath = deltaTablePath;
        this.splitAssigner = splitAssigner;
        this.enumContext = enumContext;
        this.readersAwaitingSplit = new LinkedHashMap<>();
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!enumContext.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        if (LOG.isInfoEnabled()) {
            String hostInfo =
                requesterHostname == null ? "(no host locality info)"
                    : "(on host '" + requesterHostname + "')";
            LOG.info("Subtask {} {} is requesting a file source split", subtaskId, hostInfo);
        }

        readersAwaitingSplit.put(subtaskId, requesterHostname);
        assignSplits(subtaskId);
    }

    @Override
    public void addSplitsBack(List<DeltaSourceSplit> splits, int subtaskId) {
        LOG.debug("Bounded Delta Source Enumerator adds splits back: {}", splits);
        addSplits(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    @Override
    public void close() throws IOException {
        // no resources to close
    }

    /**
     * The implementation of this method should handle case, where there is no more splits that
     * could be assigned to Source Readers.
     * <p>
     * This method is called by {@link DeltaSourceSplitEnumerator#handleSplitRequest(int, String)}
     * method.
     *
     * @param subtaskId the subtask id of the source reader who sent the source spit request event.
     */
    protected abstract void handleNoMoreSplits(int subtaskId);

    @SuppressWarnings("unchecked")
    protected Collection<DeltaSourceSplit> getRemainingSplits() {
        // The Flink's SplitAssigner interface uses FileSourceSplit
        // in its signatures.
        // This "trick" is also used in Flink source code by bundled Hive connector -
        // https://github.com/apache/flink/blob/release-1.14/flink-connectors/flink-connector-hive/src/main/java/org/apache/flink/connectors/hive/ContinuousHiveSplitEnumerator.java#L137
        return (Collection<DeltaSourceSplit>) (Collection<?>) splitAssigner.remainingSplits();
    }

    @SuppressWarnings("unchecked")
    protected void addSplits(List<DeltaSourceSplit> splits) {
        // We are doing this double cast trick here because  Flink's SplitAssigner interface uses
        // FileSourceSplit in its signatures instead something like <? extends FileSourceSplit>
        // There is no point for construction our custom Interface and Implementation
        // for splitAssigner just to have needed type.
        splitAssigner.addSplits((Collection<FileSourceSplit>) (Collection<?>) splits);
    }

    protected AssignSplitStatus assignSplits() {
        final Iterator<Entry<Integer, String>> awaitingReader =
            readersAwaitingSplit.entrySet().iterator();

        while (awaitingReader.hasNext()) {
            Entry<Integer, String> nextAwaiting = awaitingReader.next();

            // if the reader that requested another split has failed in the meantime, remove
            // it from the list of waiting readers - FLINK-20261
            if (!enumContext.registeredReaders().containsKey(nextAwaiting.getKey())) {
                awaitingReader.remove();
                continue;
            }

            String hostname = nextAwaiting.getValue();
            int awaitingSubtask = nextAwaiting.getKey();
            Optional<FileSourceSplit> nextSplit = splitAssigner.getNext(hostname);
            if (nextSplit.isPresent()) {
                FileSourceSplit split = nextSplit.get();
                enumContext.assignSplit((DeltaSourceSplit) split, awaitingSubtask);
                LOG.info("Assigned split to subtask {} : {}", awaitingSubtask, split);
                awaitingReader.remove();
            } else {
                // TODO for chunking load we will have to modify this to get a new chunk from Delta.
                return NO_MORE_SPLITS;
            }
        }

        return NO_MORE_READERS;
    }

    private void assignSplits(int subtaskId) {
        AssignSplitStatus assignSplitStatus = assignSplits();
        if (NO_MORE_SPLITS.equals(assignSplitStatus)) {
            LOG.info("No more splits available for subtasks");
            handleNoMoreSplits(subtaskId);
        }
    }

    public enum AssignSplitStatus {
        NO_MORE_SPLITS,
        NO_MORE_READERS
    }
}
