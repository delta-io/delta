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

package io.delta.kernel.types;

public enum MetadataColumnType {
  ROW_INDEX("row_index"),
  ROW_ID("row_id"),
  ROW_COMMIT_VERSION("row_commit_version");

  private final String textValue;

  MetadataColumnType(String textValue) {
    this.textValue = textValue;
  }

  public String toString() {
    return textValue;
  }

  public static MetadataColumnType fromString(String text) {
    for (MetadataColumnType type : MetadataColumnType.values()) {
      if (type.textValue.equalsIgnoreCase(text)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown MetadataColumnType: " + text);
  }
}
