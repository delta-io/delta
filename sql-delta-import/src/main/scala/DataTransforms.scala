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

package io.delta.connectors.spark.jdbc

import org.apache.spark.sql.DataFrame

/**
 * Class that applies transformation functions one by one on input DataFrame
 */
class DataTransforms(transformations: Seq[DataFrame => DataFrame]) {

  /**
   * Executes functions against DataFrame
   *
   * @param df - input DataFrame against which functions need to be executed
   * @return - modified by Seq of functions DataFrame
   */
  def runTransform(df: DataFrame): DataFrame = transformations.foldLeft(df)((v, f) => f(v))
}
