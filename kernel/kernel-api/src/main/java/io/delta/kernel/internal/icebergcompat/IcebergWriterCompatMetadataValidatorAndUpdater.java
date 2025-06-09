package io.delta.kernel.internal.icebergcompat;

import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import java.util.*;

/**
 * Contains interfaces and common utility classes performing the validations and updates necessary
 * to support the table feature IcebergWriterCompats when it is enabled by the table properties such
 * as "delta.enableIcebergWriterCompatV3".
 */
public abstract class IcebergWriterCompatMetadataValidatorAndUpdater
    extends IcebergCompatMetadataValidatorAndUpdater {
  /////////////////////////////////////////////////////////////////////////////////
  /// Interfaces for defining validations and updates necessary to support IcebergWriterCompats
  // ///
  /////////////////////////////////////////////////////////////////////////////////
  public static void validateIcebergWriterCompatChange(
      Map<String, String> oldConfig,
      Map<String, String> newConfig,
      boolean isNewTable,
      TableConfig<Boolean> writerCompatProperty) {
    if (!isNewTable) {
      boolean wasEnabled = writerCompatProperty.fromMetadata(oldConfig);
      boolean isEnabled = writerCompatProperty.fromMetadata(newConfig);
      if (!wasEnabled && isEnabled) {
        throw DeltaErrors.enablingIcebergWriterCompatV1OnExistingTable(
            writerCompatProperty.getKey());
      }
      if (wasEnabled && !isEnabled) {
        throw DeltaErrors.disablingIcebergWriterCompatV1OnExistingTable(
            writerCompatProperty.getKey());
      }
    }
  }

  protected static Optional<Metadata> validateAndUpdateIcebergWriterCompatMetadata(
      boolean isCreatingNewTable,
      Metadata newMetadata,
      Protocol newProtocol,
      IcebergWriterCompatMetadataValidatorAndUpdater instance) {
    return instance.validateAndUpdateMetadata(
        new IcebergCompatInputContext(isCreatingNewTable, newMetadata, newProtocol));
  }
}
