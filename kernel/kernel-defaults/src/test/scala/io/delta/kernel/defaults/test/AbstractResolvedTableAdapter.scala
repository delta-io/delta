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

package io.delta.kernel.defaults.test

import io.delta.kernel.{ResolvedTable, ScanBuilder, Snapshot}
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.types.StructType

/** Implicit conversions that enable easy and succinct adapter creation in tests. */
object ResolvedTableAdapterImplicits {

  /** Converts a [[Snapshot]] to a [[LegacyResolvedTableAdapter]]. */
  implicit class AdapterFromSnapshot(private val snapshot: Snapshot) extends AnyVal {
    def toTestAdapter: LegacyResolvedTableAdapter = snapshot match {
      case impl: SnapshotImpl => new LegacyResolvedTableAdapter(impl)
      case _ => throw new IllegalArgumentException("Snapshot must be an instance of SnapshotImpl")
    }
  }

  /** Converts a [[ResolvedTable]] to a [[ResolvedTableAdapter]]. */
  implicit class AdapterFromResolvedTable(private val resolvedTable: ResolvedTable) extends AnyVal {
    def toTestAdapter: ResolvedTableAdapter = new ResolvedTableAdapter(resolvedTable)
  }
}

/**
 * Test framework adapter that provides a unified interface for **accessing** Delta tables.
 *
 * This trait abstracts over the differences between [[Snapshot]] (legacy API) and [[ResolvedTable]]
 * (current API) via the [[LegacyResolvedTableAdapter]] and [[ResolvedTableAdapter]] child classes.
 */
trait AbstractResolvedTableAdapter {
  def getPath(): String
  def getVersion(): Long
  def getSchema(): StructType
  def getScanBuilder(): ScanBuilder
}

class LegacyResolvedTableAdapter(snapshot: SnapshotImpl) extends AbstractResolvedTableAdapter {
  override def getPath(): String = snapshot.getDataPath.toString
  override def getVersion(): Long = snapshot.getVersion
  override def getSchema(): StructType = snapshot.getSchema
  override def getScanBuilder(): ScanBuilder = snapshot.getScanBuilder
}

// TODO: Use ResolvedTableInternal
class ResolvedTableAdapter(rt: ResolvedTable) extends AbstractResolvedTableAdapter {
  override def getPath(): String = rt.getPath
  override def getVersion(): Long = rt.getVersion
  override def getSchema(): StructType = rt.getSchema
  override def getScanBuilder(): ScanBuilder = rt.getScanBuilder
}
