/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

import scala.collection.JavaConverters._

import io.delta.storage.LogStore
import org.apache.hadoop.conf.Configuration

import io.delta.standalone.exceptions.DeltaStandaloneException

import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.sources.StandaloneHadoopConf

private[internal] object LogStoreProvider extends LogStoreProvider

private[internal] trait LogStoreProvider {

  val logStoreClassConfKey: String = StandaloneHadoopConf.LOG_STORE_CLASS_KEY
  val defaultLogStoreClass: String = classOf[DelegatingLogStore].getName

  // The conf key for setting the LogStore implementation for `scheme`.
  def logStoreSchemeConfKey(scheme: String): String = s"delta.logStore.${scheme}.impl"

  def createLogStore(hadoopConf: Configuration): LogStore = {
    checkLogStoreConfConflicts(hadoopConf)
    val logStoreClassName = hadoopConf.get(logStoreClassConfKey, defaultLogStoreClass)
    createLogStoreWithClassName(logStoreClassName, hadoopConf)
  }

  def createLogStoreWithClassName(className: String, hadoopConf: Configuration): LogStore = {
    if (className == classOf[DelegatingLogStore].getName) {
      new DelegatingLogStore(hadoopConf)
    } else {
      // scalastyle:off classforname
      val logStoreClass =
        Class.forName(className, true, Thread.currentThread().getContextClassLoader)
      // scalastyle:on classforname

      if (classOf[LogStore].isAssignableFrom(logStoreClass)) {
        logStoreClass
          .getConstructor(classOf[Configuration])
          .newInstance(hadoopConf)
          .asInstanceOf[LogStore]
      } else {
        throw new DeltaStandaloneException(s"Can't instantiate a LogStore with classname " +
          s"$className.")
      }
    }
  }

  def checkLogStoreConfConflicts(hadoopConf: Configuration): Unit = {
    val classConf = Option(hadoopConf.get(logStoreClassConfKey))
    val schemeConf = hadoopConf.getValByRegex("""delta\.logStore\.\w+\.impl""")

    if (classConf.nonEmpty && !schemeConf.isEmpty()) {
      throw DeltaErrors.logStoreConfConflicts(schemeConf.keySet().asScala.toSeq)
    }
  }
}
