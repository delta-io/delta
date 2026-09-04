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

import org.apache.spark.sql.{DataFrame, Encoders, QueryTest}

trait DeltaInsertIntoTest
  extends QueryTest
  with DeltaDMLTestUtilsPathBased
  with org.apache.spark.sql.delta.test.DeltaSQLCommandTest
  with DeltaInsertIntoTestBase {

  override protected def beforeStreamingInsert(): Unit = {
  }

  override protected def createDataFrameFromTestData(data: TestData): DataFrame = {
    val json = spark.createDataset(data.data)(Encoders.STRING)
    spark.read.schema(data.schema).option("mode", "FAILFAST").json(json)
  }

  override protected def adaptExpectedResult(
      expectedResult: Any): DeltaInsertIntoTestHarness.ExpectedResult[Any] = {
    expectedResult match {
      case ExpectedResult.Success(expected) =>
        DeltaInsertIntoTestHarness.ExpectedResult.Success(expected)
      case ExpectedResult.Failure(checkError) =>
        DeltaInsertIntoTestHarness.ExpectedResult.Failure(checkError)
      case _ =>
        super.adaptExpectedResult(expectedResult)
    }
  }
}
