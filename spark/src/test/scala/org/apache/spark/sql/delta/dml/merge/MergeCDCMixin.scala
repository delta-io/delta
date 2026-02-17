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

package org.apache.spark.sql.delta.dml.merge

import org.apache.spark.sql.delta.columnmapping.DeltaColumnMappingTestUtils
import org.apache.spark.sql.delta.deletionvectors.MergePersistentDVDisabled
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.test.SharedSparkSession

/**
 * Mixin trait for Merge CDC tests.
 * Extracted from MergeCDCSuite.scala to break circular dependencies.
 */
trait MergeCDCMixin extends SharedSparkSession
  with MergeIntoSQLTestUtils
  with DeltaColumnMappingTestUtils
  with DeltaSQLCommandTest
  with MergePersistentDVDisabled
