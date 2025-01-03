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
import io.delta.kernel.internal.data.DelegateRow;
import io.delta.kernel.types.*;
import java.util.Collections;
import java.util.Map;

/**
 * An abstract base class for Delta Log actions that are backed by a {@link Row}. This design is to
 * avoid materialization of all fields when creating action instances from action rows within
 * Kernel. Actions like {@link AddFile} can extend this class to maintain just a reference to the
 * underlying action row.
 */
public abstract class RowBackedAction {

  /** The underlying {@link Row} that represents an action and contains all its field values. */
  protected final Row row;

  protected RowBackedAction(Row row, StructType expectedSchema) {
    checkArgument(
        row.getSchema().equals(expectedSchema),
        "Expected row schema: %s, found: %s",
        expectedSchema,
        row.getSchema());

    this.row = row;
  }

  /**
   * Returns the index of the field with the given name in the full schema of the row. Throws an
   * {@link IllegalArgumentException} if the field is not found.
   */
  protected int getFieldIndex(String fieldName) {
    int index = row.getSchema().indexOf(fieldName);
    checkArgument(index >= 0, "Field '%s' not found in schema: %s", fieldName, row.getSchema());
    return index;
  }

  /**
   * Returns a new {@link Row} with the same schema and values as the row backing this action, but
   * with the value of the field with the given name overridden by the given value.
   */
  protected Row toRowWithOverriddenValue(String fieldName, Object value) {
    Map<Integer, Object> overrides = Collections.singletonMap(getFieldIndex(fieldName), value);
    return new DelegateRow(row, overrides);
  }

  /** Returns the underlying {@link Row} that represents this action. */
  public final Row toRow() {
    return row;
  }
}
