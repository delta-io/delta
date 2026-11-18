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

package org.apache.spark.sql.delta.amt

import org.apache.spark.sql.delta.DeltaInsertIntoTests

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * Generates AMT variants of the INSERT test suites built on [[DeltaInsertIntoTests]], which run
 * INSERT through [[DeltaInsertIntoTests.doInsert]].
 *
 * Each INSERT through `doInsert` is bracketed by the [[AMTDMLTestUtils]] checkpoints. The INSERTs
 * some tests run directly in SQL or through the DataFrame writer still write AMT tables, but
 * without the checkpoints.
 *
 * The INSERT AMT mixin should come after all other mixins of a suite, so its dimension should be
 * the last of every generator config it is used in. This is for 2 reasons:
 * 1. Visibility: some mixins declare beforeAll public, and a later mixin overriding it as
 *    protected (which AMTDMLTestUtils does) fails to compile. This is a latent issue for the INSERT
 *    suites.
 * 2. Commit orders: some mixins perform extra commits. To maintain a stable commit order, we
 *    prefer to have AMT commits wrap them all.
 */
trait InsertAMTMixin extends DeltaInsertIntoTests with AMTDMLTestUtils {


  abstract override protected def doInsert(
      tableName: String,
      insert: DataFrame,
      mode: SaveMode): Unit = {
    withAMTCheckpointsAround(tableName) {
      super.doInsert(tableName, insert, mode)
    }
  }
}
