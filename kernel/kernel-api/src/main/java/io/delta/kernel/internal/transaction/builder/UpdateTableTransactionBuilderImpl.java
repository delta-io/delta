package io.delta.kernel.internal.transaction.builder;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.actions.SetTransaction;
import io.delta.kernel.internal.table.ResolvedTableInternal;
import io.delta.kernel.internal.transaction.TransactionFactory;
import io.delta.kernel.internal.transaction.builder.context.ExistingTableBuilderContext;
import io.delta.kernel.transaction.Transaction;
import io.delta.kernel.transaction.UpdateTableTransactionBuilder;
import io.delta.kernel.types.StructType;
import java.util.Optional;
import java.util.Set;

public class UpdateTableTransactionBuilderImpl
    extends BaseTransactionBuilderImpl<UpdateTableTransactionBuilder>
    implements UpdateTableTransactionBuilder {

  private final ExistingTableBuilderContext existingContext;
  private Optional<StructType> updatedSchemaOpt = Optional.empty();
  private Optional<Set<String>> unsetTablePropertiesKeysOpt = Optional.empty();

  public UpdateTableTransactionBuilderImpl(ResolvedTableInternal baseTable) {
    super();
    this.existingContext = new ExistingTableBuilderContext(baseTable);
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
    existingContext.validateKernelCanWriteToTable();
    validateSetTblPropertiesAndUnsetTblProperties();
    validateNotEnablingClusteringOnPartitionedTable();

    return TransactionFactory.createTransaction(
        existingContext.getBaseTable().getPath(),
        existingContext.getBaseTable().getVersion(),
        existingContext.getBaseTable().getTimestamp(),
        existingContext.getBaseTable().getProtocol(),
        existingContext.getBaseTable().getMetadata()); // should we merge in the schema here???
  }

  private void validateSetTblPropertiesAndUnsetTblProperties() {
    if (unsetTablePropertiesKeysOpt.isPresent() && newTablePropertiesOpt.isPresent()) {
      Set<String> invalidPropertyKeys =
          unsetTablePropertiesKeysOpt.get().stream()
              .filter(key -> newTablePropertiesOpt.get().containsKey(key))
              .collect(toSet());
      if (!invalidPropertyKeys.isEmpty()) {
        throw DeltaErrors.overlappingTablePropertiesSetAndUnset(invalidPropertyKeys);
      }
    }
  }

  private void validateNotEnablingClusteringOnPartitionedTable() {}
}
