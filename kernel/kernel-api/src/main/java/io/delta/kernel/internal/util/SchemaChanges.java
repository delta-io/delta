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
package io.delta.kernel.internal.util;

import io.delta.kernel.types.StructField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * SchemaChanges encapsulates a list of added, removed, renamed, or updated fields in a schema
 * change. Updates include reordered fields, type changes, nullability changes, and metadata
 * attribute changes. This set of updates can apply to nested fields within structs. In case any
 * update is applied to a nested field, an update will be produced for every level of nesting. This
 * includes re-ordered columns in a nested field. Note that SchemaChanges does not capture
 * re-ordered columns in top level schema.
 *
 * <p>For example, given a field struct_col: struct<inner_struct<id: int>> if id is renamed to
 * `renamed_id` 1 update will be produced for the change to struct_col and 1 update will be produced
 * for the change to inner_struct
 *
 * <p>ToDo: Possibly track moves/renames independently, enable capturing re-ordered columns in top
 * level schema
 */
class SchemaChanges<T extends Supplier<StructField>> {
  private final List<T> addedFields;
  private final List<T> removedFields;
  private final List<T> renamedFields;
  private final List<Tuple2<T, T>> updatedFields;

  private SchemaChanges(
      List<T> addedFields,
      List<T> removedFields,
      List<Tuple2<T, T>> updatedFields,
      List<T> renamedFields) {
    this.addedFields = Collections.unmodifiableList(addedFields);
    this.removedFields = Collections.unmodifiableList(removedFields);
    this.updatedFields = Collections.unmodifiableList(updatedFields);
    this.renamedFields = Collections.unmodifiableList(renamedFields);
  }

  static class Builder<T extends Supplier<StructField>> {
    private final List<T> addedFields = new ArrayList<>();
    private final List<T> removedFields = new ArrayList<>();
    private final List<T> renamedFields = new ArrayList<>();
    private final List<Tuple2<T, T>> updatedFields = new ArrayList<>();

    public Builder<T> withAddedField(T addedField) {
      addedFields.add(addedField);
      return this;
    }

    public Builder<T> withRemovedField(T removedField) {
      removedFields.add(removedField);
      return this;
    }

    public Builder<T> withUpdatedField(T existingField, T newField) {
      updatedFields.add(new Tuple2<>(existingField, newField));
      return this;
    }

    public Builder<T> withRenamedField(T renamedField) {
      renamedFields.add(renamedField);
      return this;
    }

    public SchemaChanges<T> build() {
      return new SchemaChanges<T>(addedFields, removedFields, updatedFields, renamedFields);
    }
  }

  public static <T extends Supplier<StructField>> Builder<T> builder() {
    return new Builder<>();
  }

  /* Added Fields */
  public List<T> addedFields() {
    return addedFields;
  }

  /* Removed Fields */
  public List<T> removedFields() {
    return removedFields;
  }

  /* Updated Fields (e.g. rename, type change) represented as a Tuple<FieldBefore, FieldAfter> */
  public List<Tuple2<T, T>> updatedFields() {
    return updatedFields;
  }

  /* Returns fields which are only renamed */
  public List<T> renamedFields() {
    return renamedFields;
  }
}
