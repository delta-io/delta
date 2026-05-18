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

package org.apache.spark.sql.delta.catalog

import org.apache.spark.sql.connector.catalog.TableCatalog

/**
 * No-op shim of `ChangelogSupport` for Spark 4.1.
 *
 * <p>The catalog-driven `TableCatalog.loadChangelog` entrypoint and its supporting types
 * (`Changelog`, `ChangelogInfo`, `ChangelogRange`) were introduced in Spark 4.2 via
 * SPARK-56685. They do not exist in Spark 4.0/4.1, so the Auto-CDF wiring is compiled in only
 * when building against Spark 4.2 (see `scala-shims/spark-4.2/...ChangelogSupport.scala`).
 *
 * <p>In 4.0/4.1 builds, mixing this empty trait into `DeltaCatalog` is a no-op: there is no
 * `loadChangelog` to override, and downstream Auto-CDF classes (`DeltaChangelog`, etc.) live in
 * version-specific `java-shims/spark-4.2/` dirs and are not present here either.
 */
trait ChangelogSupport extends TableCatalog
