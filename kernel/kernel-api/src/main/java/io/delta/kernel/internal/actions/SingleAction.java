/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.actions;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.types.StructType;
import java.util.Collections;

public class SingleAction {
  /**
   * Get the schema of reading entries from Delta Log delta and checkpoint files for construction of
   * new checkpoint.
   */
  public static StructType CHECKPOINT_SCHEMA =
      new StructType()
          .add("txn", SetTransaction.FULL_SCHEMA)
          .add("add", AddFile.FULL_SCHEMA)
          .add("remove", RemoveFile.FULL_SCHEMA)
          .add("metaData", Metadata.FULL_SCHEMA)
          .add("protocol", Protocol.FULL_SCHEMA)
          .add("domainMetadata", DomainMetadata.FULL_SCHEMA);

  // Once we start supporting updating CDC or domain metadata enabled tables, we should add the
  // schema for those fields here.

  /**
   * Schema to use when reading the winning commit files for conflict resolution. This schema is
   * just for resolving conflicts when doing a blind append. It doesn't cover case when the txn is
   * reading data from the table and updating the table.
   */
  public static StructType CONFLICT_RESOLUTION_SCHEMA =
      new StructType()
          .add("txn", SetTransaction.FULL_SCHEMA)
          // .add("add", AddFile.FULL_SCHEMA) // not needed for blind appends
          // .add("remove", RemoveFile.FULL_SCHEMA) // not needed for blind appends
          .add("metaData", Metadata.FULL_SCHEMA)
          .add("protocol", Protocol.FULL_SCHEMA)
          .add("commitInfo", CommitInfo.FULL_SCHEMA)
          .add("domainMetadata", DomainMetadata.FULL_SCHEMA);

  // Once we start supporting domain metadata/row tracking enabled tables, we should add the
  // schema for domain metadata fields here.

  // Schema to use when writing out the single action to the Delta Log.
  public static StructType FULL_SCHEMA =
      new StructType()
          .add("txn", SetTransaction.FULL_SCHEMA)
          .add("add", AddFile.FULL_SCHEMA)
          .add("remove", RemoveFile.FULL_SCHEMA)
          .add("metaData", Metadata.FULL_SCHEMA)
          .add("protocol", Protocol.FULL_SCHEMA)
          .add("cdc", new StructType())
          .add("commitInfo", CommitInfo.FULL_SCHEMA)
          .add("domainMetadata", DomainMetadata.FULL_SCHEMA);
  // Once we start supporting updating CDC or domain metadata enabled tables, we should add the
  // schema for those fields here.

  public static final int TXN_ORDINAL = FULL_SCHEMA.indexOf("txn");
  public static final int ADD_FILE_ORDINAL = FULL_SCHEMA.indexOf("add");
  public static final int REMOVE_FILE_ORDINAL = FULL_SCHEMA.indexOf("remove");
  public static final int METADATA_ORDINAL = FULL_SCHEMA.indexOf("metaData");
  public static final int PROTOCOL_ORDINAL = FULL_SCHEMA.indexOf("protocol");
  public static final int COMMIT_INFO_ORDINAL = FULL_SCHEMA.indexOf("commitInfo");
  private static final int DOMAIN_METADATA_ORDINAL = FULL_SCHEMA.indexOf("domainMetadata");

  public static Row createAddFileSingleAction(Row addFile) {
    return new GenericRow(FULL_SCHEMA, Collections.singletonMap(ADD_FILE_ORDINAL, addFile));
  }

  public static Row createProtocolSingleAction(Row protocol) {
    return new GenericRow(FULL_SCHEMA, Collections.singletonMap(PROTOCOL_ORDINAL, protocol));
  }

  public static Row createMetadataSingleAction(Row metadata) {
    return new GenericRow(FULL_SCHEMA, Collections.singletonMap(METADATA_ORDINAL, metadata));
  }

  public static Row createRemoveFileSingleAction(Row remove) {
    return new GenericRow(FULL_SCHEMA, Collections.singletonMap(REMOVE_FILE_ORDINAL, remove));
  }

  public static Row createCommitInfoSingleAction(Row commitInfo) {
    return new GenericRow(FULL_SCHEMA, Collections.singletonMap(COMMIT_INFO_ORDINAL, commitInfo));
  }

  public static Row createDomainMetadataSingleAction(Row domainMetadata) {
    return new GenericRow(
        FULL_SCHEMA, Collections.singletonMap(DOMAIN_METADATA_ORDINAL, domainMetadata));
  }

  public static Row createTxnSingleAction(Row txn) {
    return new GenericRow(FULL_SCHEMA, Collections.singletonMap(TXN_ORDINAL, txn));
  }
}
