/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta

import org.apache.spark.SparkThrowable

/**
 * The trait for all exceptions of Delta code path.
 */
trait DeltaThrowable extends SparkThrowable { // some change 4
  // Portable error identifier across SQL engines
  // If null, error class or SQLSTATE is not set
  override def getSqlState: String =
    DeltaThrowableHelper.getSqlState(this.getErrorClass.split('.').head)

  // True if this error is an internal error.
  override def isInternalError: Boolean = DeltaThrowableHelper.isInternalError(this.getErrorClass)
}
