package io.delta.flink.source.internal.enumerator.processor;

import java.util.List;

import io.delta.flink.source.internal.enumerator.monitor.ChangesPerVersion;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.file.AddFileEnumerator.SplitFilter;
import io.delta.flink.source.internal.file.AddFileEnumeratorContext;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.utils.SourceUtils;
import org.apache.flink.core.fs.Path;

import io.delta.standalone.actions.AddFile;

public abstract class BaseTableProcessor implements TableProcessor {

    /**
     * A {@link Path} to Delta Table that this processor reads.
     */
    protected final Path deltaTablePath;

    /**
     * The {@code AddFileEnumerator}'s to convert all discovered {@link AddFile} to set of {@link
     * DeltaSourceSplit}.
     */
    protected final AddFileEnumerator<DeltaSourceSplit> fileEnumerator;

    public BaseTableProcessor(
        Path deltaTablePath, AddFileEnumerator<DeltaSourceSplit> fileEnumerator) {
        this.deltaTablePath = deltaTablePath;
        this.fileEnumerator = fileEnumerator;
    }

    protected AddFileEnumeratorContext setUpEnumeratorContext(List<AddFile> addFiles,
        long snapshotVersion) {
        String pathString = SourceUtils.pathToString(deltaTablePath);
        return new AddFileEnumeratorContext(pathString, addFiles, snapshotVersion);
    }

    protected List<DeltaSourceSplit> prepareSplits(
        ChangesPerVersion<AddFile> changes, SplitFilter<Path> splitFilter) {
        AddFileEnumeratorContext context =
            setUpEnumeratorContext(changes.getChanges(), changes.getSnapshotVersion());
        return fileEnumerator.enumerateSplits(context, splitFilter);
    }
}
