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

package org.apache.spark.sql.delta.catalog.v2

import org.apache.spark.sql.delta.catalog.DeltaCatalog

import org.apache.spark.sql.connector.catalog.TransactionalCatalogPlugin
import org.apache.spark.sql.connector.catalog.transactions.{Transaction => SparkTransaction, TransactionInfo => SparkTransactionInfo}

/**
 * Mixes v2 transactional execution into a [[DeltaCatalog]]. Implementing
 * [[TransactionalCatalogPlugin]] allows Spark to begin and manage a [[DeltaV2SparkTransaction]].
 */
private[catalog] trait DeltaV2TransactionalCatalogSupport extends TransactionalCatalogPlugin {
  self: DeltaCatalog =>

  override def beginTransaction(info: SparkTransactionInfo): SparkTransaction =
    new DeltaV2SparkTransaction(self, info)
}

/**
 * Transactional variant of [[DeltaCatalog]]: the same catalog with v2 transactional execution
 * mixed in via [[DeltaV2TransactionalCatalogSupport]]. See [[DeltaCatalog]] for the base catalog
 * behavior and configuration. This class only adds the v2 transaction opt-in.
 *
 * A session opts in by registering this catalog in place of [[DeltaCatalog]]. That choice is fixed
 * for the session's lifetime. Disabling requires registering the plain [[DeltaCatalog]] and
 * starting a new session.
 */
private[catalog] class DeltaV2TransactionalCatalog
  extends DeltaCatalog with DeltaV2TransactionalCatalogSupport
