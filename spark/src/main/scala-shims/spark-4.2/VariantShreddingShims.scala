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

package org.apache.spark.sql.delta.shims

import org.apache.spark.sql.internal.SQLConf

/**
 * Shim for variant shredding configs to handle API changes between Spark versions.
 * In Spark 4.2, VARIANT_INFER_SHREDDING_SCHEMA config exists.
 *
 * This shim provides a way to conditionally add the config to the options map
 * when writing files.
 */
object VariantShreddingShims {
  /**
   * Returns a Map containing variant shredding related configs for file writing.
   * In Spark 4.2, this returns the VARIANT_INFER_SHREDDING_SCHEMA config.
   */
  def getVariantInferShreddingSchemaOptions(enableVariantShredding: Boolean): Map[String, String] = {
    Map(SQLConf.VARIANT_INFER_SHREDDING_SCHEMA.key -> enableVariantShredding.toString)
  }
}
