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

package io.delta.flink.sink.dynamic;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;

/**
 * Keys writer committables by target Delta table so each aggregator subtask owns disjoint tables.
 */
final class TableKeySelector
    implements KeySelector<CommittableMessage<DeltaDynamicWriterResult>, String>,
        java.io.Serializable {

  @Override
  public String getKey(CommittableMessage<DeltaDynamicWriterResult> value) {
    return ((CommittableWithLineage<DeltaDynamicWriterResult>) value)
        .getCommittable()
        .getTableName();
  }
}
