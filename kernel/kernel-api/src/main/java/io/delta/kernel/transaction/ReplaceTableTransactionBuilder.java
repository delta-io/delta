package io.delta.kernel.transaction;

import io.delta.kernel.expressions.Column;
import java.util.List;

/**
 * NOTE: Schema is a required parameter when creating
 * {@link io.delta.kernel.internal.transaction.builder.ReplaceTableTransactionBuilderImpl}
 */
public interface ReplaceTableTransactionBuilder 
    extends BaseTransactionBuilder<ReplaceTableTransactionBuilder> {

  ReplaceTableTransactionBuilder withPartitionColumns(List<Column> partitionColumns);
}
