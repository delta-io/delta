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

/**
 * SchemaChanges encapsulates a list of added, removed, renamed, or updated fields in a schema
 * change. Updates include renamed fields, reordered fields, type changes, nullability changes, and
 * metadata attribute changes. This set of updates can apply to nested fields within structs. In
 * case any update is applied to a nested field, an update will be produced for every level of
 * nesting. This includes re-ordered columns in a nested field. Note that SchemaChanges does not
 * capture re-ordered columns in top level schema.
 *
 * <p>For example, given a field struct_col: struct<inner_struct<id: int>> if id is renamed to
 * `renamed_id` 1 update will be produced for the change to struct_col and 1 update will be produced
 * for the change to inner_struct
 *
 * <p>ToDo: Possibly track moves/renames independently, enable capturing re-ordered columns in top
 * level schema
 */
class SchemaChanges {
  private List<StructField> addedFields;
  private List<StructField> removedFields;
  private List<Tuple2<StructField, StructField>> updatedFields;

  private SchemaChanges(
      List<StructField> addedFields,
      List<StructField> removedFields,
      List<Tuple2<StructField, StructField>> updatedFields) {
    this.addedFields = Collections.unmodifiableList(addedFields);
    this.removedFields = Collections.unmodifiableList(removedFields);
    this.updatedFields = Collections.unmodifiableList(updatedFields);
  }

  static class Builder {
    private List<StructField> addedFields = new ArrayList<>();
    private List<StructField> removedFields = new ArrayList<>();
    private List<Tuple2<StructField, StructField>> updatedFields = new ArrayList<>();

    public Builder withAddedField(StructField addedField) {
      addedFields.add(addedField);
      return this;
    }

    public Builder withRemovedField(StructField removedField) {
      removedFields.add(removedField);
      return this;
    }

    public Builder withUpdatedField(StructField existingField, StructField newField) {
      updatedFields.add(new Tuple2<>(existingField, newField));
      return this;
    }

    public SchemaChanges build() {
      return new SchemaChanges(addedFields, removedFields, updatedFields);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  /* Added Fields */
  public List<StructField> addedFields() {
    return addedFields;
  }

  /* Removed Fields */
  public List<StructField> removedFields() {
    return removedFields;
  }

  /* Updated Fields (e.g. rename, type change) represented as a Tuple<FieldBefore, FieldAfter> */
  public List<Tuple2<StructField, StructField>> updatedFields() {
    return updatedFields;
  }
}
