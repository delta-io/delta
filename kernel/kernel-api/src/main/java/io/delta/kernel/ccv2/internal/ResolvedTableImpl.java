package io.delta.kernel.ccv2.internal;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.ccv2.ResolvedMetadata;
import io.delta.kernel.ccv2.ResolvedTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResolvedTableImpl implements ResolvedTable {

  private static final Logger logger = LoggerFactory.getLogger(ResolvedTableImpl.class);

  private final ResolvedMetadata resolvedMetadata;
  private final Snapshot snapshot;

  public ResolvedTableImpl(ResolvedMetadata resolvedMetadata) {
    validateResolvedMetadata(resolvedMetadata);

    this.resolvedMetadata = resolvedMetadata;
    this.snapshot = null;
  }


  /////////////////
  // Public APIs //
  /////////////////

  @Override
  public Snapshot getSnapshot() {
    return snapshot;
  }

  @Override
  public TransactionBuilder createTransactionBuilder(String engineInfo, Operation operation) {
    return null;
  }

  ////////////////////
  // Helper methods //
  ////////////////////

  private void validateResolvedMetadata(ResolvedMetadata rm) {
    requireNonNull(rm, "resolvedMetadata is null");
    requireNonNull(rm.getPath(), "ResolvedMetadata.getPath() is null");
    requireNonNull(rm.getLogSegment(), "ResolvedMetadata.getLogSegment() is null");
    requireNonNull(rm.getProtocol(), "ResolvedMetadata.getProtocol() is null");
    requireNonNull(rm.getMetadata(), "ResolvedMetadata.getMetadata() is null");
    requireNonNull(rm.getSchemaString(), "ResolvedMetadata.getSchemaString() is null");

    checkArgument(
        rm.getProtocol().isPresent() == rm.getMetadata().isPresent(),
        "Protocol and Metadata must be present or absent together");

    rm.getLogSegment().ifPresent(logSegment -> {
      checkArgument(
          logSegment.getVersion() == rm.getVersion(),
          "ResolvedMetadata.getVersion() does not match the version of the LogSegment");
    });
  }
}
