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

import org.apache.spark.sql.delta.metering.DeltaLogging

/**
 * Scala inheritance bridge for the Java [[DeltaV2Table]]. This lets the table use the scoped
 * profiling helpers on [[DeltaLogging]] without duplicating them in a companion object.
 */
private[catalog] abstract class DeltaV2TableLogging
  extends DeltaV2TableShims
  with DeltaLogging
