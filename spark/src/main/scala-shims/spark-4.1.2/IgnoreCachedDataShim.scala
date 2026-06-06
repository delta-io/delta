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
package org.apache.spark.sql.catalyst.plans.logical

/**
 * Shim for Spark's `IgnoreCachedData` trait. SPARK-54812 removed the trait and changed
 * `CacheManager` to automatically skip any `Command`-derived plan during cache replacement.
 * That change was backported to Spark 4.1.2, so this post-backport shim is an empty marker.
 * All Delta commands that mix this in already extend `RunnableCommand`/`Command`, so they are
 * covered by the new automatic path.
 */
trait IgnoreCachedDataShim
