package io.delta.kernel.internal.transaction;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.Tuple2;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TransactionFactory {

  public TransactionFactory TransactionFactory createTransactionFctory(
      String tablePath,
      long readTableVersion,
      long readTableTimestamp,
      Protocol baseProtocol,
      Metadata baseMetadata) {
    Metadata newMetadata = baseMetadata;
    newMetadata = updateMetadataWithTablePropertyAdditions(newMetadata);
    newMetadata = updateMetadataWithTablePropertyRemovals(newMetadata);

    return null;
  }

  static Metadata updateMetadataWithTablePropertyAdditions(Metadata input, Optional<Map<String, String>> newTablePropertiesOpt) {
    return null;
  }

  static Metadata updateMetadataWithTablePropertyRemovals(Metadata input, Optional<Set<String>> unsetTablePropertiesKeysOpt) {
    return null;
  }

  // TODO: don't have this; UPDATE should just apply this itself before???
  static Metadata updateMetadataWithSchemaForUpdateOnly(Metadata input, )
}
