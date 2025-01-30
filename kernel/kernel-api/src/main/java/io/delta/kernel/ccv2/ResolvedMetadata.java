package io.delta.kernel.ccv2;

import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.utils.CloseableIterable;
import java.util.Optional;

public interface ResolvedMetadata {

  /////////////////////////////////////////////////
  // APIs to provide input information to Kernel //
  /////////////////////////////////////////////////

  /** @return The fully qualified path of the table. */
  String getPath();

  /** @return The resolved version of the table. */
  long getVersion();

  /**
   * @return A potentially partial {@link LogSegment}. If any deltas are present, then they
   *     represent a contiguous suffix of commits up until the given version.
   */
  Optional<LogSegment> getLogSegment();

  /**
   * @return The protocol of the table at the given version. If present, then {@link #getMetadata()}
   *     must also be present.
   */
  Optional<Protocol> getProtocol();

  /**
   * @return The metadata of the table at the given version. If present, then {@link #getProtocol()}
   *     must also be present.
   */
  Optional<Metadata> getMetadata();

  /** @return The schema of the table, at the given version, as a JSON-serialized string. */
  Optional<String> getSchemaString();

  /////////////////////////////////////////////////////////////
  // APIs for Kernel to interact with the invoking connector //
  /////////////////////////////////////////////////////////////

  /** Commit the given data actions to the table. */
  TransactionCommitResult commit(
      CloseableIterable<Row> dataActions,
      Optional<Protocol> newProtocol,
      Optional<Metadata> newMetadata);
}
