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

package io.delta.standalone.internal.scan

import java.util.{NoSuchElementException, Optional}

import io.delta.standalone.DeltaScan
import io.delta.standalone.actions.{AddFile => AddFileJ}
import io.delta.standalone.data.CloseableIterator
import io.delta.standalone.expressions.Expression
import io.delta.standalone.internal.actions.AddFile
import io.delta.standalone.internal.util.ConversionUtils

/**
 * Scala implementation of Java interface [[DeltaScan]].
 *
 * TODO this is currently a naive implementation, since
 * a) it takes in the in-memory AddFiles.
 * b) it uses the metadata.partitionColumns, but the metadata won't be known until the log files
 *    are scanned
 */
private[internal] class DeltaScanImpl(files: Seq[AddFile]) extends DeltaScan {

  /**
   * Whether or not the given [[addFile]] should be returned during iteration.
   */
  protected def accept(addFile: AddFile): Boolean = true

  /**
   * This is a utility method for internal use cases where we need the filtered files
   * as their Scala instances, instead of Java.
   *
   * Since this is for internal use, we can keep this as a [[Seq]].
   */
  def getFilesScala: Seq[AddFile] = files.filter(accept)

  // TODO: memory-optimized implementation
  override def getFiles: CloseableIterator[AddFileJ] = new CloseableIterator[AddFileJ] {
    private var nextValid: Option[AddFile] = None
    private val iter = files.iterator

    // Initialize next valid element so that the first hasNext() and next() calls succeed
    findNextValid()

    private def findNextValid(): Unit = {
      while (iter.hasNext) {
        val next = iter.next()
        if (accept(next)) {
          nextValid = Some(next)
          return
        }
      }

      // No next valid found
      nextValid = None
    }

    override def hasNext: Boolean = {
      nextValid.isDefined
    }

    override def next(): AddFileJ = {
      if (!hasNext) throw new NoSuchElementException()
      val ret = ConversionUtils.convertAddFile(nextValid.get)
      findNextValid()
      ret
    }

    override def close(): Unit = { }
  }

  override def getPushedPredicate: Optional[Expression] = Optional.empty()

  override def getResidualPredicate: Optional[Expression] = Optional.empty()
}
