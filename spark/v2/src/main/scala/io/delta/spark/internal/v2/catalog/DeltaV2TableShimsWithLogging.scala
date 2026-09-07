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
package io.delta.spark.internal.v2.catalog

import io.delta.spark.internal.v2.DeltaV2Logging

/**
 * Scala inheritance bridge that combines the Java table's Spark-version shims with scoped Delta
 * V2 logging.
 */
private[catalog] abstract class DeltaV2TableShimsWithLogging
  extends DeltaV2TableShims
  with DeltaV2Logging
