/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.internal;

import static io.delta.kernel.internal.DeltaErrors.requireSchemaForReplaceTable;

import io.delta.kernel.Operation;
import io.delta.kernel.Transaction;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class ReplaceTableTransactionBuilderImpl extends TransactionBuilderImpl {

  public ReplaceTableTransactionBuilderImpl(TableImpl table, String engineInfo) {
    super(table, engineInfo, Operation.REPLACE_TABLE);
  }

  @Override
  public Transaction build(Engine engine) {
    try {
      withMaxRetries(0); // We don't support conflict resolution yet so disable retries for now
      schema.orElseThrow(() -> requireSchemaForReplaceTable(table.getPath(engine)));
      // TODO we need to validate the schema:
      //   When re-using fieldIds we need to check that type & nullability is the same, otherwise
      //   do not allow fieldId re-use and throw an error
      SnapshotImpl snapshot = (SnapshotImpl) table.getLatestSnapshot(engine);
      return buildTransactionInternal(engine, true, Optional.of(snapshot));
    } catch (TableNotFoundException tblf) {
      throw new TableNotFoundException(
          tblf.getTablePath(), "Trying to replace a table that does not exist.");
    }
  }

  /*
  Generally for replace table we want to reset all table state, however there are a few
  delta-specific properties that we should preserve
  */
  protected static Set<String> tablePropertyKeysToPreserve =
      new HashSet<String>() {
        {
          add(TableConfig.COLUMN_MAPPING_MAX_COLUMN_ID.getKey());
          // TODO are there any other table properties we should preserve?
        }
      };
}
