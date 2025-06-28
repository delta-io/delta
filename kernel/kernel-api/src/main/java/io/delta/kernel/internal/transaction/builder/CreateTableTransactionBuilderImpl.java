package io.delta.kernel.internal.transaction.builder;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.transaction.TransactionFactory;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.Transaction;
import io.delta.kernel.types.StructType;
import java.util.List;
import java.util.Optional;

public class CreateTableTransactionBuilderImpl
    extends CreateLikeTransactionBuilder
    implements CreateTableTransactionBuilder {

  private final String path;
  private Optional<StructType> schemaOpt = Optional.empty();
  private Optional<List<Column>> partitionColumnsOpt = Optional.empty();
  private boolean domainMetadataSupported = false;

  public CreateTableTransactionBuilderImpl(String path, StructType schema) {
    super(schema);
    this.path = path;
  }

  ////////////////////////////////////////
  // BaseTransactionBuilderImpl methods //
  ////////////////////////////////////////

  @Override
  protected CreateTableTransactionBuilderImpl self() {
    return this;

  }

  ////////////////////////////////////////
  // BaseTransactionBuilderImpl methods //
  ////////////////////////////////////////

  @Override
  public Transaction build(Engine engine) {
    // TODO: validate transaction inputs
    validatePathBasedTableDoesNotExist();
    return TransactionFactory.createTransaction(
        path,
        -1,
        -1,
        getInitialProtocol(),
        getInitialMetadata()
    );
  }
  
  private void validatePathBasedTableDoesNotExist() {}
}
