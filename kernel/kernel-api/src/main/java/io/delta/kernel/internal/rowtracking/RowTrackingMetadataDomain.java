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
package io.delta.kernel.internal.rowtracking;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.metadatadomain.JsonMetadataDomain;
import java.util.Optional;

/** Represents the metadata domain for row tracking. */
public final class RowTrackingMetadataDomain extends JsonMetadataDomain {

  public static final String DOMAIN_NAME = "delta.rowTracking";

  public static final long MISSING_ROW_ID_HIGH_WATERMARK = -1L;

  /**
   * Creates an instance of {@link RowTrackingMetadataDomain} from a JSON configuration string.
   *
   * @param json the JSON configuration string
   * @return an instance of {@link RowTrackingMetadataDomain}
   */
  public static RowTrackingMetadataDomain fromJsonConfiguration(String json) {
    return JsonMetadataDomain.fromJsonConfiguration(json, RowTrackingMetadataDomain.class);
  }

  /**
   * Creates an instance of {@link RowTrackingMetadataDomain} from a {@link SnapshotImpl}.
   *
   * @param snapshot the snapshot instance
   * @return an {@link Optional} containing the {@link RowTrackingMetadataDomain} if present
   */
  public static Optional<RowTrackingMetadataDomain> fromSnapshot(SnapshotImpl snapshot) {
    return JsonMetadataDomain.fromSnapshot(snapshot, RowTrackingMetadataDomain.class, DOMAIN_NAME);
  }

  /** The highest assigned fresh row id for the table */
  private final long rowIdHighWaterMark;

  /**
   * Constructs a RowTrackingMetadataDomain with the specified row ID high water mark.
   *
   * @param rowIdHighWaterMark the row ID high water mark
   */
  @JsonCreator
  public RowTrackingMetadataDomain(@JsonProperty("rowIdHighWaterMark") long rowIdHighWaterMark) {
    this.rowIdHighWaterMark = rowIdHighWaterMark;
  }

  @Override
  public String getDomainName() {
    return DOMAIN_NAME;
  }

  public long getRowIdHighWaterMark() {
    return rowIdHighWaterMark;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    RowTrackingMetadataDomain that = (RowTrackingMetadataDomain) obj;
    return rowIdHighWaterMark == that.rowIdHighWaterMark;
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(DOMAIN_NAME, rowIdHighWaterMark);
  }

  @Override
  public String toString() {
    return "RowTrackingMetadataDomain{" + "rowIdHighWaterMark=" + rowIdHighWaterMark + '}';
  }
}
