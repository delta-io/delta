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

import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.implicits.RichSparkClasses
import org.apache.spark.sql.delta.util.DeltaEncoders

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

package object implicits extends DeltaEncoders with RichSparkClasses {
  // Define a few implicit classes to provide the `toDF` method. These classes are not using generic
  // types to avoid touching Scala reflection.
  implicit class RichAddFileSeq(files: Seq[AddFile]) {
    def toDF(spark: SparkSession): DataFrame = spark.implicits.localSeqToDatasetHolder(files).toDF

    def toDS(spark: SparkSession): Dataset[AddFile] =
      spark.implicits.localSeqToDatasetHolder(files).toDS
  }

  implicit class RichStringSeq(strings: Seq[String]) {
    def toDF(spark: SparkSession): DataFrame = spark.implicits.localSeqToDatasetHolder(strings).toDF

    def toDF(spark: SparkSession, colNames: String*): DataFrame =
      spark.implicits.localSeqToDatasetHolder(strings).toDF(colNames: _*)
  }

  implicit class RichIntSeq(ints: Seq[Int]) {
    def toDF(spark: SparkSession): DataFrame = spark.implicits.localSeqToDatasetHolder(ints).toDF

    def toDF(spark: SparkSession, colNames: String*): DataFrame =
      spark.implicits.localSeqToDatasetHolder(ints).toDF(colNames: _*)
  }
}
