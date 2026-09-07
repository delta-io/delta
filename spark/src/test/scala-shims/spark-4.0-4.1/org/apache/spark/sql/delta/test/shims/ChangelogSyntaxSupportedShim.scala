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

package org.apache.spark.sql.delta.test.shims

/**
 * Shim indicating whether the running Spark supports the `SELECT ... CHANGES` clause used by the
 * V2 changelog read path. Spark 4.0 and 4.1 lack the parser support, so this variant returns
 * false, letting changelog tests cancel. The spark-4.2 variant returns true.
 */
trait ChangelogSyntaxSupportedShim {
  def supportsChangelogSyntax: Boolean = false
}
