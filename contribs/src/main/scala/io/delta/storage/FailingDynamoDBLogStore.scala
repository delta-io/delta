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

package io.delta.storage
import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration

class FailingDynamoDBLogStore(sparkConf: SparkConf, hadoopConf: Configuration)
    extends DynamoDBLogStore(sparkConf, hadoopConf) {

  private val injectErrors: Boolean = true
  private val errorRates = {
    val rates = sparkConf
      .get(s"${DynamoDBLogStore.confPrefix}errorRates", "")
      .split(',')
      .filter(s => s.contains('='))
      .map(v => v.split("=", 2))
      .map(v => (v(0), v(1).toFloat))
      .toMap
    logInfo(s"errorRates: ${rates}")
    rates
  }

  private val rng: java.util.Random = new java.util.Random()

  override def onWriteCopyTempFile(): Unit = injectError("write_copy_temp_file")

  override def onWritePutDbEntry(): Unit = injectError("write_put_db_entry")

  override def onFixDeltaLogCopyTempFile(): Unit = injectError(
    "fix_delta_log_copy_temp_file"
  )

  override def onFixDeltaLogPutDbEntry(): Unit = injectError(
    "fix_delta_log_copy_temp_file"
  )

  private def injectError(name: String): Unit = {
    assert(
      rng.nextFloat() >= errorRates.get(name).getOrElse(0.0f)
    )
  }
}
