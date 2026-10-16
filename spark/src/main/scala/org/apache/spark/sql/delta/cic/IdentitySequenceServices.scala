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

package org.apache.spark.sql.delta.cic

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.cic.IdentitySequenceService

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

/**
 * Resolves the [[IdentitySequenceService]] backend for a session. When
 * [[DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_SERVICE_CLASS_NAME]] is set, reflectively instantiates
 * that class (no-arg constructor); whether a table uses a sequence service at all stays
 * table-state-driven (the stamped sequence pointer).
 *
 * Otherwise there is no built-in backend, so it fails with a clear error directing the caller to
 * configure a backend via the class-name config.
 */
object IdentitySequenceServices {
  def resolve(spark: SparkSession): IdentitySequenceService = {
    spark.conf.get(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_SERVICE_CLASS_NAME) match {
      case Some(className) if className.nonEmpty =>
        Utils.classForName(className).getDeclaredConstructor().newInstance()
          .asInstanceOf[IdentitySequenceService]
      case _ =>
        throw new UnsupportedOperationException(
          "No concurrent identity-sequence backend is configured. Set " +
            "spark.databricks.delta.identityColumn.concurrent.serviceClassName to an " +
            "IdentitySequenceService implementation.")
    }
  }
}
