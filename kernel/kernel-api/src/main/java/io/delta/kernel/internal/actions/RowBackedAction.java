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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;

/**
 * Base abstract class for Delta Log actions that are backed by a {@link Row}.This abstraction is
 * used by actions like {@link AddFile}, where we want to avoid materializing all fields from the
 * row when creating an action instance. For these actions, we only maintain a reference to the
 * underlying row, and the getters retrieve values directly from it.
 */
public abstract class RowBackedAction {

  /** The underlying {@link Row} that represents an action and contains all its field values. */
  protected final Row row;

  protected RowBackedAction(Row row) {
    checkArgument(
        row.getSchema().equals(getFullSchema()),
        "Expected row schema: %s, found: %s",
        getFullSchema(),
        row.getSchema());

    this.row = row;
  }

  /** Returns the full schema of the row that represents this action. */
  protected abstract StructType getFullSchema();

  public Row toRow() {
    return row;
  }
}
