/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package io.delta.standalone.internal.storage

import io.delta.standalone.storage.LogStore
import io.delta.standalone.internal.sources.StandaloneHadoopConf

import org.apache.hadoop.conf.Configuration

private[internal] object LogStoreProvider extends LogStoreProvider

private[internal] trait LogStoreProvider {

  val defaultLogStoreClassName: String = classOf[HDFSLogStore].getName

  def createLogStore(hadoopConf: Configuration): LogStore = {
    val logStoreClassName =
      hadoopConf.get(StandaloneHadoopConf.LOG_STORE_CLASS_KEY, defaultLogStoreClassName)

    // scalastyle:off classforname
    val logStoreClass =
      Class.forName(logStoreClassName, true, Thread.currentThread().getContextClassLoader)
    // scalastyle:on classforname

    if (classOf[LogStore].isAssignableFrom(logStoreClass)) {
      logStoreClass
        .getConstructor(classOf[Configuration])
        .newInstance(hadoopConf)
        .asInstanceOf[LogStore]
    } else {
      // TODO proper error?
      throw new IllegalArgumentException(s"Can't instantiate a LogStore with classname " +
        s"$logStoreClassName.")
    }
  }
}
