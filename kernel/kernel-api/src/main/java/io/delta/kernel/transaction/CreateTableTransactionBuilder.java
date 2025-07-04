package io.delta.kernel.transaction;

import io.delta.kernel.expressions.Column;
import java.util.List;

/**
 * NOTE: Schema is a required parameter when creating
 * {@link io.delta.kernel.internal.transaction.builder.CreateTableTransactionBuilderImpl}
 */
public interface CreateTableTransactionBuilder
    extends BaseTransactionBuilder<CreateTableTransactionBuilder> {

  CreateTableTransactionBuilder withPartitionColumns(List<Column> partitionColumns);
}
