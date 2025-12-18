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

package org.apache.spark.sql.delta.logging

import java.util.Locale

import org.apache.spark.internal.LogKey

/**
 * Shim for LogKey to handle API changes between Spark versions.
 * In Spark 4.1, LogKey is a Java interface requiring explicit implementation of `name()`.
 *
 * DeltaLogKey provides the implementation of name() that case objects can inherit.
 */
abstract class DeltaLogKey extends LogKey {
  override def name(): String = getClass.getSimpleName.stripSuffix("$").toLowerCase(Locale.ROOT)
}
