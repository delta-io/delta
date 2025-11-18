package io.delta.flink.source.internal.file;

import java.util.List;

import io.delta.standalone.actions.AddFile;

/**
 * This class provides a context and input for {@link AddFileEnumerator} needed to convert {@link
 * AddFile} to Splits.
 */
public class AddFileEnumeratorContext {

    /**
     * Path to Delta table for which this context is created.
     */
    private final String tablePath;

    /**
     * A list of {@link AddFile} that should be converted to Splits in scope of this context.
     */
    private final List<AddFile> addFiles;

    /**
     * A Delta table snapshot version that this context represents.
     */
    private final long snapshotVersion;

    /**
     * Creates {@code AddFileEnumeratorContext} for given {@code tablePath} and {@code addFiles}
     * list. The {@code AddFileEnumeratorContext} is expected to have a version scope thus it should
     * contain {@code AddFile}'s only from one version.
     *
     * @param tablePath       A path for Delta table for witch this context was created.
     * @param addFiles        A list of {@link AddFile} that should be converted to Splits and are
     *                        coming from {@code tablePath}.
     * @param snapshotVersion A {@link io.delta.standalone.Snapshot} version for which this context
     *                        was created.
     */
    public AddFileEnumeratorContext(String tablePath, List<AddFile> addFiles,
        long snapshotVersion) {
        this.tablePath = tablePath;
        this.addFiles = addFiles;
        this.snapshotVersion = snapshotVersion;
    }

    /**
     * @return Path to Delta Table for which this context is created.
     */
    public String getTablePath() {
        return tablePath;
    }

    /**
     * @return A list of {@link AddFile} that should be converted to Splits in scope of this
     * context.
     */
    public List<AddFile> getAddFiles() {
        return addFiles;
    }

    /**
     * @return A Delta Table snapshot version that this context represents.
     */
    public long getSnapshotVersion() {
        return snapshotVersion;
    }
}
