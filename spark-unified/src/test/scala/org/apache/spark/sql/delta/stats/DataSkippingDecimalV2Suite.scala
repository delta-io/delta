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

package org.apache.spark.sql.delta.stats

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.V2ForceTest

/**
 * Tests decimal precision mismatch handling through the V2/Kernel data skipping path.
 *
 * When V2_ENABLE_MODE=STRICT, filter predicates are pushed through SparkScanBuilder into
 * the Kernel's data skipping evaluator. If a query literal has a different decimal
 * precision/scale than the column type (e.g., DECIMAL(10,3) literal vs DECIMAL(18,6)
 * column), the Kernel's ImplicitCastExpression must support DECIMAL-to-DECIMAL widening.
 * Without that support, data skipping throws UnsupportedOperationException.
 */
class DataSkippingDecimalV2Suite
  extends QueryTest
  with V2ForceTest {

  test("decimal precision mismatch in filter should not fail with V2 connector") {
    withTempDir { dir =>
      val tablePath = dir.getCanonicalPath

      // Write data using V1 connector (V2 doesn't support writes yet).
      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "NONE") {
        spark.sql(s"""
          CREATE TABLE delta.`$tablePath` (id INT, price DECIMAL(18, 6)) USING delta
        """)
        // File 1: low values
        spark.sql(s"""
          INSERT INTO delta.`$tablePath` VALUES (1, 100.500000), (2, 150.250000)
        """)
        // File 2: high values
        spark.sql(s"""
          INSERT INTO delta.`$tablePath` VALUES (3, 500.750000), (4, 600.100000)
        """)
      }

      // Reads go through V2 connector (V2ForceTest sets STRICT mode).
      // Query with DECIMAL(10,3) literal vs column DECIMAL(18,6).
      // Without the kernel fix, this throws UnsupportedOperationException during
      // data skipping in DefaultExpressionEvaluator.transformBinaryComparator().

      // Greater-than: should skip File 1 and return only File 2 rows
      checkAnswer(
        spark.sql(s"""
          SELECT id FROM delta.`$tablePath`
          WHERE price > CAST(200.0 AS DECIMAL(10, 3))
          ORDER BY id
        """),
        Seq(Row(3), Row(4))
      )

      // Less-than: should skip File 2 and return only File 1 rows
      checkAnswer(
        spark.sql(s"""
          SELECT id FROM delta.`$tablePath`
          WHERE price < CAST(200.0 AS DECIMAL(10, 3))
          ORDER BY id
        """),
        Seq(Row(1), Row(2))
      )

      // Equality with exact value but different precision
      checkAnswer(
        spark.sql(s"""
          SELECT id FROM delta.`$tablePath`
          WHERE price = CAST(100.500 AS DECIMAL(10, 3))
          ORDER BY id
        """),
        Seq(Row(1))
      )
    }
  }
}
