package io.delta.kernel.internal.transaction.builder;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.SetTransaction;
import io.delta.kernel.internal.table.ResolvedTableInternal;
import io.delta.kernel.internal.transaction.TransactionFactory;
import io.delta.kernel.transaction.Transaction;
import io.delta.kernel.transaction.UpdateTableTransactionBuilder;
import io.delta.kernel.types.StructType;
import java.util.Optional;
import java.util.Set;

public class UpdateTableTransactionBuilderImpl
    extends BaseTransactionBuilderImpl<UpdateTableTransactionBuilder>
    implements UpdateTableTransactionBuilder {

  private final ResolvedTableInternal baseTable;
  private Optional<StructType> updatedSchemaOpt = Optional.empty();
  private Optional<Set<String>> unsetTablePropertiesKeysOpt = Optional.empty();

  public UpdateTableTransactionBuilderImpl(ResolvedTableInternal baseTable) {
    super();
    this.baseTable = baseTable;
  }

  ////////////////////////////////////////
  // BaseTransactionBuilderImpl methods //
  ////////////////////////////////////////

  @Override
  protected UpdateTableTransactionBuilderImpl self() {
    return this;
  }

  ///////////////////////////////////////////
  // UpdateTableTransactionBuilder methods //
  ///////////////////////////////////////////

  @Override
  public UpdateTableTransactionBuilder withSetTransactionId(String applicationId, long transactionVersion) {
    SetTransaction txnId =
        new SetTransaction(
            requireNonNull(applicationId, "applicationId is null"),
            transactionVersion,
            Optional.of(currentTimeMillis));
    this.setTxnOpt = Optional.of(txnId);
    return this;
  }

  @Override
  public UpdateTableTransactionBuilder withUpdatedSchema(StructType updatedSchema) {
    this.updatedSchemaOpt = Optional.of(updatedSchema);
    return this;
  }

  @Override
  public UpdateTableTransactionBuilder withTablePropertiesRemoved(Set<String> propertyKeys) {
    if (!propertyKeys.isEmpty()) {
      this.unsetTablePropertiesKeysOpt = Optional.of(propertyKeys);
    }
    return this;
  }

  ////////////////////////////////////////
  // BaseTransactionBuilderImpl methods //
  ////////////////////////////////////////

  @Override
  public Transaction build(Engine engine) {
    // TODO: validate transaction inputs
    return TransactionFactory.createTransaction(
        baseTable.getPath(),
        baseTable.getVersion(),
        baseTable.getTimestamp(),
        baseTable.getProtocol(),
        baseTable.getMetadata()
    );
  }
}
