package io.delta.flink.source.internal.builder;

import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;

/**
 * Delta representation of Flink's {@link BulkFormat} for {@link DeltaSourceSplit}
 *
 * @param <T> Type of element produced by created {@link DeltaBulkFormat}
 */
public interface DeltaBulkFormat<T> extends BulkFormat<T, DeltaSourceSplit> {

}
