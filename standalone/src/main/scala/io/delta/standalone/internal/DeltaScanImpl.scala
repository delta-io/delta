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

package io.delta.standalone.internal

import java.util.Optional

import io.delta.standalone.DeltaScan
import io.delta.standalone.actions.{AddFile => AddFileJ}
import io.delta.standalone.data.CloseableIterator
import io.delta.standalone.expressions.Expression

/**
 * Scala implementation of Java interface [[DeltaScan]].
 *
 * TODO this is currently a naive implementation, since
 * a) it takes in the in-memory AddFiles.
 * b) it uses the metadata.partitionColumns, but the metadata won't be known until the log files
 *    are scanned
 */
final class DeltaScanImpl(
    files: java.util.List[AddFileJ],
    expr: Option[Expression] = None) extends DeltaScan {

  override def getFiles: CloseableIterator[AddFileJ] = new CloseableIterator[AddFileJ] {
    private val iter = files.iterator

    override def hasNext: Boolean = iter.hasNext

    override def next(): AddFileJ = iter.next()

    override def close(): Unit = { }
  }

  override def getPushedPredicate: Optional[Expression] = Optional.empty()

  override def getResidualPredicate: Optional[Expression] = Optional.ofNullable(expr.orNull)

}
