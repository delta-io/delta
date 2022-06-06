package io.delta.flink.source.internal.builder;

import java.util.List;

/**
 * An interface for {@link DeltaBulkFormat} builder implementations.
 *
 * @param <T> Type of element produced by created {@link DeltaBulkFormat}
 */
public interface FormatBuilder<T> {

    DeltaBulkFormat<T> build();

    FormatBuilder<T> partitionColumns(List<String> partitionColumns);

    FormatBuilder<T> parquetBatchSize(int size);
}
