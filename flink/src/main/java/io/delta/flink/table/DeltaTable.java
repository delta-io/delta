/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.flink.table;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A {@code DeltaTable} represents a logical view of a Delta table and provides access to both table
 * metadata (such as schema and partitioning) and operations for reading and writing table data.
 *
 * <p>A {@code DeltaTable} instance abstracts the underlying Delta transaction log and storage
 * layout. Implementations are responsible for:
 *
 * <ul>
 *   <li>exposing immutable table metadata (schema, partition information)
 *   <li>managing transaction boundaries and versioned commits
 *   <li>coordinating reads and writes against the physical table storage
 *   <li>serializing table changes into Delta {@code actions} and committing them atomically
 * </ul>
 *
 * <p>All implementations must be {@link Serializable} to allow use in distributed execution
 * environments.
 */
public interface DeltaTable extends Serializable, AutoCloseable {

  /**
   * Returns a stable identifier that uniquely represents this table within its catalog or storage
   * system.
   *
   * <p>These are some examples that may be used as identifiers, depending on the subclass
   * implementation and the catalog in use.
   *
   * <ul>
   *   <li>a logical table name (e.g., {@code "catalog.database.table"})
   *   <li>a filesystem or object-store URI
   * </ul>
   *
   * @return a unique logical identifier for the table
   */
  String getId();

  /**
   * Returns the table schema as a {@link StructType}.
   *
   * <p>The schema defines the logical column structure of the table. Implementations should
   * guarantee that the schema corresponds to the latest committed version unless otherwise
   * documented.
   *
   * @return the table schema
   */
  StructType getSchema();

  /**
   * Returns the list of partition columns for this table.
   *
   * <p>The returned list defines the physical partitioning strategy used by the table. The ordering
   * of columns follows the table’s partition specification and should be stable across versions.
   *
   * @return an ordered list of partition column names
   */
  List<String> getPartitionColumns();

  /**
   * Init the table instance and make it ready for use. Must be called at least once before the
   * table can be safely used. Calling open on an already opened table has no effect.
   */
  void open();

  /**
   * Commits a new version to the table by applying the provided Delta actions.
   *
   * <p>Actions may include (but are not limited to):
   *
   * <ul>
   *   <li>{@code AddFile} records representing new data files
   *   <li>{@code RemoveFile} records removing obsolete files
   *   <li>metadata updates or protocol changes
   * </ul>
   *
   * <p>Implementations must ensure atomicity: either all provided actions are committed as part of
   * a new table version, or none are. Commit conflicts should be detected and surfaced as
   * exceptions.
   *
   * @param actions an iterable collection of Delta actions to commit; the caller is responsible for
   *     closing the iterable
   * @param appId application id used for this commit. See transaction identifier in Delta protocol.
   * @param txnId the transaction identifier to be used for this commit.
   * @param properties table properties to be updated with this commit.
   */
  Optional<Snapshot> commit(
      CloseableIterable<Row> actions, String appId, long txnId, Map<String, String> properties);

  /**
   * Refreshes the table state by reloading the latest snapshot metadata.
   *
   * <p>This method updates the in-memory view of the table to reflect the most recently committed
   * version, including:
   *
   * <ul>
   *   <li>the latest table schema,
   *   <li>partition column definitions, and
   *   <li>any other metadata derived from the current Delta log snapshot.
   * </ul>
   *
   * <p>{@code refresh()} should be invoked when external changes to the table may have occurred
   * (for example, commits from other writers) and the caller requires an up-to-date view before
   * performing read or write operations.
   *
   * <p>Implementations may perform I/O and metadata parsing as part of this operation.
   */
  void refresh();

  /**
   * Writes one or more Parquet files as part of the table and emits the corresponding {@code
   * AddFile} action describing the newly written data.
   *
   * <p>This operation is responsible for:
   *
   * <ul>
   *   <li>writes to the underlying storage layer using the specified {@code data}
   *   <li>constructing physical file paths by appending {@code pathSuffix} to the table root
   *   <li>materializing partition values into the file metadata
   *   <li>returning a Row describing the resulting {@code AddFile} action
   * </ul>
   *
   * <p>The returned iterator typically contains exactly one row (the AddFile action), but
   * implementations may return multiple actions depending on file-splitting behavior.
   *
   * @param pathSuffix a suffix appended to the table path when generating file locations. The
   *     result path will be `<table_root>/<path_suffix>/<paquet_file>`
   * @param data an iterator over row batches to be written as Parquet files; this method will close
   *     it on consumption.
   * @param partitionValues a mapping of partition column names to their literal values
   * @return an iterator over {@code Row} objects representing the AddFile actions generated during
   *     the write
   * @throws IOException if data writing or file creation fails
   */
  CloseableIterator<Row> writeParquet(
      String pathSuffix,
      CloseableIterator<FilteredColumnarBatch> data,
      Map<String, Literal> partitionValues)
      throws IOException;
}
