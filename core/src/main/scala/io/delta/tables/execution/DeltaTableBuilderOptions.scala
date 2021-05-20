/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package io.delta.tables.execution

/**
 * DeltaTableBuilder option to indicate whether it's to create / replace the table.
 */
sealed trait DeltaTableBuilderOptions

/**
 * Specify that the builder is to create a Delta table.
 *
 * @param ifNotExists boolean whether to ignore if the table already exists.
 */
case class CreateTableOptions(ifNotExists: Boolean) extends DeltaTableBuilderOptions

/**
 * Specify that the builder is to replace a Delta table.
 *
 * @param orCreate boolean whether to create the table if the table doesn't exist.
 */
case class ReplaceTableOptions(orCreate: Boolean) extends DeltaTableBuilderOptions


