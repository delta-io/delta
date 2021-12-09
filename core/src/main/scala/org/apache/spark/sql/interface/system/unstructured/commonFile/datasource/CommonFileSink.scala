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
package org.apache.spark.sql.interface.system.unstructured.commonFile.datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}



/**
 * Author: CHEN ZHI LING
 * Date: 2021/8/17
 * Description:
 */
class CommonFileSink(sqlContext: SQLContext, path: String) extends Sink with DeltaLogging{



  /**
   * save every record to delta in Append mode
   */
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val query: QueryExecution = data.queryExecution
    val rdd: RDD[InternalRow] = query.toRdd
    val df: DataFrame = sqlContext.internalCreateDataFrame(rdd, data.schema)
    df.write.format("delta").mode(SaveMode.Append).save(path)
  }
}
