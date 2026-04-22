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
package org.apache.spark.sql.catalyst.plans.logical

/**
 * Shim for Spark's `IgnoreCachedData` trait. In Spark 4.2 (SPARK-54812) the
 * trait was removed because `CacheManager` now automatically skips any
 * `Command`-derived plan during cache replacement. This empty marker trait
 * keeps Delta commands source-compatible across Spark versions without
 * affecting behavior on 4.2+: all Delta commands that previously mixed in
 * `IgnoreCachedData` already extend `RunnableCommand`/`Command` and are
 * therefore handled by the new automatic path.
 */
trait IgnoreCachedDataShim
