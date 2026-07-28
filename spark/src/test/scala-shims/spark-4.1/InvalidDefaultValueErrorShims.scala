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

package org.apache.spark.sql.delta.test.shims

/**
 * Test shim for INVALID_DEFAULT_VALUE error codes that changed between Spark versions.
 * In Spark 4.1, the error code is INVALID_DEFAULT_VALUE.UNRESOLVED_EXPRESSION
 */
object InvalidDefaultValueErrorShims {
  val INVALID_DEFAULT_VALUE_ERROR_CODE: String = "INVALID_DEFAULT_VALUE.UNRESOLVED_EXPRESSION"
}

