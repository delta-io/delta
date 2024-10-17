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

  // We accept keys with the `spark.` prefix to maintain compatibility with delta-spark
  val acceptedLogStoreClassConfKeyRegex =
    f"((?:spark.)?${StandaloneHadoopConf.LOG_STORE_CLASS_KEY}|" +
    f"${StandaloneHadoopConf.LEGACY_LOG_STORE_CLASS_KEY})"
      .replace(""".""", """\.""")
  val acceptedLogStoreSchemeConfKeyRegex = """(?:spark\.)?delta\.logStore\.\w+\.impl"""

  val logStoreClassConfKey: String = StandaloneHadoopConf.LOG_STORE_CLASS_KEY
  val defaultLogStoreClass: String = classOf[DelegatingLogStore].getName

  // The conf key for setting the LogStore implementation for `scheme`.
  def logStoreSchemeConfKey(scheme: String): String = s"delta.logStore.${scheme}.impl"

  def createLogStore(hadoopConf: Configuration): LogStore = {
    checkLogStoreConfConflicts(hadoopConf)
    normalizeHadoopConf(hadoopConf)
    val logStoreClassName = hadoopConf.get(logStoreClassConfKey, defaultLogStoreClass)
    createLogStoreWithClassName(logStoreClassName, hadoopConf)
  }

  def createLogStoreWithClassName(className: String, hadoopConf: Configuration): LogStore = {
    if (className == classOf[DelegatingLogStore].getName) {
      new DelegatingLogStore(hadoopConf)
    } else {
      // scalastyle:off classforname
      // Do not pass a null class loader
      // - https://github.com/netty/netty/issues/7290
      // - https://bugs.openjdk.java.net/browse/JDK-7008595
      val classLoader: java.lang.ClassLoader = Option(Thread.currentThread().getContextClassLoader)
        .getOrElse(this.getClass().getClassLoader)
      val logStoreClass =
        Class.forName(className, true, classLoader)
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

  /**
   * Normalizes LogStore hadoop configs.
   * - For each config, check that the values are consistent across all accepted keys. Throw an
   *   error if they are not.
   * - Set the "normalized" key to such value. This means future accesses can exclusively use the
   *   normalized keys.
   *
   * For scheme conf keys:
   * - We accept 'delta.logStore.{scheme}.impl' and 'spark.delta.logStore.{scheme}.impl'
   * - The normalized key is 'delta.logStore.{scheme}.impl'
   *
   * For class conf key:
   * - We accept 'delta.logStore.class', 'spark.delta.logStore.class', and
   *   'io.delta.standalone.LOG_STORE_CLASS_KEY' (legacy).
   * - The normalized key is 'delta.logStore.class'
   */
  def normalizeHadoopConf(hadoopConf: Configuration): Unit = {
    // LogStore scheme conf keys
    val schemeConfs = hadoopConf.getValByRegex(acceptedLogStoreSchemeConfKeyRegex).asScala
    schemeConfs.filter(_._1.startsWith("spark.")).foreach { case (key, value) =>
      val normalizedKey = key.stripPrefix("spark.")
      Option(hadoopConf.get(normalizedKey)) match {
        case Some(normalValue) =>
          // The normalized key is also present in the hadoopConf. Check that they store
          // the same value, otherwise throw an error.
          if (value != normalValue) {
            throw DeltaErrors.inconsistentLogStoreConfs(
              Seq((key, value), (normalizedKey, normalValue)))
          }
        case None =>
          // The normalized key is not present in the hadoopConf. Set the normalized key to the
          // provided value.
          hadoopConf.set(normalizedKey, value)
      }
    }

    // LogStore class conf key
    val classConfs = hadoopConf.getValByRegex(acceptedLogStoreClassConfKeyRegex).asScala
    if (classConfs.values.toSet.size > 1) {
      // More than one class conf key are set to different values
      throw DeltaErrors.inconsistentLogStoreConfs(classConfs.iterator.toSeq)
    } else if (classConfs.size > 0) {
      // Set the normalized key to the provided value.
      hadoopConf.set(logStoreClassConfKey, classConfs.values.head)
    }
  }

  def checkLogStoreConfConflicts(hadoopConf: Configuration): Unit = {
    val classConf = hadoopConf.getValByRegex(acceptedLogStoreClassConfKeyRegex)
    val schemeConf = hadoopConf.getValByRegex(acceptedLogStoreSchemeConfKeyRegex)

    if (!classConf.isEmpty() && !schemeConf.isEmpty()) {
      throw DeltaErrors.logStoreConfConflicts(
        classConf.keySet().asScala.toSeq,
        schemeConf.keySet().asScala.toSeq)
    }
  }
}
