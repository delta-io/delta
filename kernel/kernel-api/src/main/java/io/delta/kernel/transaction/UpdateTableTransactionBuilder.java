package io.delta.kernel.transaction;

import io.delta.kernel.types.StructType;
import java.util.Set;

public interface UpdateTableTransactionBuilder
    extends BaseTransactionBuilder<UpdateTableTransactionBuilder> {

  UpdateTableTransactionBuilder withSetTransactionId(String applicationId, long transactionVersion);

  UpdateTableTransactionBuilder withUpdatedSchema(StructType updatedSchema);

  UpdateTableTransactionBuilder withTablePropertiesRemoved(Set<String> propertyKeys);
}
