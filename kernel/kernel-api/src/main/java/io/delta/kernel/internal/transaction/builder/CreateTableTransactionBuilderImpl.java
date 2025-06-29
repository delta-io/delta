package io.delta.kernel.internal.transaction.builder;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.transaction.TransactionFactory;
import io.delta.kernel.internal.transaction.builder.context.CreateLikeBuilderContext;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.Transaction;
import io.delta.kernel.types.StructType;
import java.util.List;

public class CreateTableTransactionBuilderImpl
    extends BaseTransactionBuilderImpl<CreateTableTransactionBuilder>
    implements CreateTableTransactionBuilder {

  private final String path;
  private final CreateLikeBuilderContext createHelper;
  private boolean domainMetadataSupported = false;

  public CreateTableTransactionBuilderImpl(String path, StructType schema) {
    super();
    createHelper = new CreateLikeBuilderContext(schema);
    this.path = path;
  }

  @Override
  public CreateTableTransactionBuilderImpl withPartitionColumns(List<Column> partitionColumns) {
    createHelper.withPartitionColumns(partitionColumns);
    return this;
  }

  @Override
  protected CreateTableTransactionBuilderImpl self() {
    return this;

  }

  @Override
  public Transaction build(Engine engine) {
    createHelper.validateCreateLikeInputs();

    return TransactionFactory.createTransaction(
        path, -1, -1, createHelper.getInitialProtocol(), createHelper.getInitialMetadata());
  }


}
