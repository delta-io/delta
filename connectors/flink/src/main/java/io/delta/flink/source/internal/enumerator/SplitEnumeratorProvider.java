package io.delta.flink.source.internal.enumerator;

import java.io.Serializable;

import io.delta.flink.internal.options.DeltaConnectorConfiguration;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;

/**
 * Factory for {@link SplitEnumerator}.
 */
public interface SplitEnumeratorProvider extends Serializable {

    /**
     * Creates {@link SplitEnumerator} instance.
     * <p>
     * This method should be used when creating the {@link SplitEnumerator} instance for the first
     * time or without a Flink's checkpoint data. This method will be called from {@link
     * org.apache.flink.api.connector.source.Source#createEnumerator(SplitEnumeratorContext)}.
     *
     * @param deltaTablePath      {@link Path} for Delta table.
     * @param configuration       Hadoop Configuration that should be used to read Parquet files.
     * @param enumContext         {@link SplitEnumeratorContext}.
     * @param sourceConfiguration {@link DeltaConnectorConfiguration} used for creating Delta
     * Source.
     * @return {@link SplitEnumerator} instance.
     */
    SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>>
        createInitialStateEnumerator(Path deltaTablePath, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaConnectorConfiguration sourceConfiguration);


    /**
     * Creates {@link SplitEnumerator} instance from {@link DeltaEnumeratorStateCheckpoint} data.
     * <p>
     * This method should be used when creating the {@link SplitEnumerator} instance during Flink's
     * recovery from a checkpoint. This method will be called from {@link
     * org.apache.flink.api.connector.source.Source#restoreEnumerator(SplitEnumeratorContext,
     * Object)}.
     *
     * @param checkpoint          {@link DeltaEnumeratorStateCheckpoint} that should be used to
     *                            create {@link SplitEnumerator} instance.
     * @param configuration       Hadoop Configuration that should be used to read Parquet files.
     * @param enumContext         {@link SplitEnumeratorContext}.
     * @param sourceConfiguration {@link DeltaConnectorConfiguration} used for creating Delta
     *  Source.
     * @return {@link SplitEnumerator} instance.
     */
    SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>>
        createEnumeratorForCheckpoint(
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaConnectorConfiguration sourceConfiguration);

    /**
     * @return {@link Boundedness} type for {@link SplitEnumerator} created by this provider.
     */
    Boundedness getBoundedness();

}
