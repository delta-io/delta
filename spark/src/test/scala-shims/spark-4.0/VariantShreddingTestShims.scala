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
 * Test shim for variant shredding to handle differences between Spark versions.
 * In Spark 4.0, VARIANT_INFER_SHREDDING_SCHEMA does not exist.
 */
object VariantShreddingTestShims {
  /**
   * Returns true if VARIANT_INFER_SHREDDING_SCHEMA config is supported in this Spark version.
   * In Spark 4.0, this returns false.
   */
  val variantInferShreddingSchemaSupported: Boolean = false

  /**
   * Returns a dummy config key for VARIANT_INFER_SHREDDING_SCHEMA.
   * In Spark 4.0, since this config doesn't exist, we return a dummy key that won't affect tests.
   * This allows tests to compile but the config will have no effect.
   */
  val variantInferShreddingSchemaKey: String = "spark.sql.dummy.variantInferShreddingSchema"
}
