package io.delta.kernel.internal.transaction;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.transaction.Transaction;
import io.delta.kernel.types.StructType;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TransactionFactory {

  public static Transaction createTransaction(
      String tablePath,
      long readTableVersion,
      long readTableTimestamp,
      Protocol baseProtocol,
      Metadata baseMetadata) {
    Metadata newMetadata = baseMetadata;
    Protocol newProtocol = baseProtocol;
    newMetadata = updateMetadataWithTablePropertyAdditions(newMetadata);
    newMetadata = updateMetadataWithTablePropertyRemovals(newMetadata);
    newMetadata = updateMetadataWithSchemaForUpdateOnly(newMetadata, null); // TODO: do this in update

    return null;
  }

  static Metadata updateMetadataWithTablePropertyAdditions(Metadata input, Optional<Map<String, String>> newTablePropertiesOpt) {
    return null;
  }

  static Metadata updateMetadataWithTablePropertyRemovals(Metadata input, Optional<Set<String>> unsetTablePropertiesKeysOpt) {
    return null;
  }

  // TODO: don't have this; UPDATE should just apply this itself before???
  static Metadata updateMetadataWithSchemaForUpdateOnly(Metadata input, StructType newSchema) {
    return null;
  }

  static Protocol upgradeProtocol() {}

  static Protocol
}
