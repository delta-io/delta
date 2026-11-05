/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import java.net.URI

import scala.collection.mutable.ArrayBuffer

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

/**
 * In-memory GoogleHadoopFileSystem for unit-testing the fast listFrom path without GCS.
 *
 * Reproduces the listStatusStartingFrom semantics observed in gcs-connector 4.0.4: a flat,
 * bucket-wide, lexicographically ordered listing of keys >= the start path (inclusive), at most
 * one raw page per call with directory placeholders (keys ending in '/') filtered AFTER the page
 * is cut, paging past all-placeholder pages. Canned bucket content is supplied through the
 * companion object (FileSystem.get instantiates this class reflectively), and every
 * listStatusStartingFrom invocation is recorded for assertions.
 */
class FakeGoogleHadoopFileSystem extends GoogleHadoopFileSystem {
  import FakeGoogleHadoopFileSystem._

  private var fsUri: URI = _

  // Deliberately does not call super.initialize: no credentials or GCS client exist in unit
  // tests; every method used by the tests is overridden below.
  override def initialize(name: URI, conf: Configuration): Unit = {
    fsUri = URI.create(s"${name.getScheme}://${name.getAuthority}")
    setConf(conf)
  }

  override def getUri: URI = fsUri

  override def getWorkingDirectory: Path = new Path(fsUri.toString + "/")

  override def makeQualified(path: Path): Path =
    path.makeQualified(getUri, getWorkingDirectory)

  override def exists(f: Path): Boolean = {
    val k = key(f)
    k.isEmpty || keys.exists(x => x == k || x == s"$k/" || x.startsWith(s"$k/"))
  }

  override def listStatusStartingFrom(hadoopPath: Path): Array[FileStatus] = {
    listStartingFromCalls += key(hadoopPath)
    maybeThrowOnListStartingFrom.foreach(t => throw t)
    // Like GoogleCloudStorageImpl.listStorageObjects: cut a raw page, filter directory
    // placeholders AFTER the cut, and keep fetching raw pages until at least one object
    // survives (so an empty result really means nothing is left).
    var start = key(hadoopPath)
    while (true) {
      val rawPage = keys.filter(_ >= start).take(pageSize)
      if (rawPage.isEmpty) {
        return Array.empty
      }
      val survivors = rawPage.filterNot(_.endsWith("/"))
      if (survivors.nonEmpty) {
        return survivors.map(fileStatus).toArray
      }
      start = rawPage.last + 0.toChar // continue after the raw page, like the page token does
    }
    throw new IllegalStateException("unreachable")
  }

  override def listStatus(f: Path): Array[FileStatus] = {
    listStatusCalls += key(f)
    val dirKey = key(f) match {
      case k if k.isEmpty || k.endsWith("/") => k
      case k => s"$k/"
    }
    val children = keys
      .filter(_.startsWith(dirKey))
      .map { k =>
        val rest = k.substring(dirKey.length)
        val slash = rest.indexOf('/')
        if (slash >= 0) Left(rest.substring(0, slash)) else Right(k)
      }
    val subDirs = children.collect { case Left(d) => d }.distinct
      .map(d => dirStatus(dirKey + d))
    val files = children.collect { case Right(k) if !k.endsWith("/") => fileStatus(k) }
    (subDirs ++ files).toArray
  }

  private def key(p: Path): String = {
    val k = p.toUri.getPath
    if (k.startsWith("/")) k.substring(1) else k
  }

  private def fileStatus(k: String): FileStatus =
    new FileStatus(11L, false, 1, 128L, 1723500000000L, new Path(s"$fsUri/$k"))

  private def dirStatus(k: String): FileStatus =
    new FileStatus(0L, true, 1, 0L, 0L, new Path(s"$fsUri/$k"))
}

object FakeGoogleHadoopFileSystem {
  /** All object keys in the fake bucket, in lexicographic order (like a real GCS listing). */
  @volatile var keys: Seq[String] = Seq.empty
  /** Raw page size, i.e. the fs.gs.list.max.items.per.call equivalent. */
  @volatile var pageSize: Int = 1000
  /** When set, listStatusStartingFrom throws it (to test the LinkageError fallback). */
  @volatile var maybeThrowOnListStartingFrom: Option[Throwable] = None

  val listStartingFromCalls: ArrayBuffer[String] = ArrayBuffer.empty
  val listStatusCalls: ArrayBuffer[String] = ArrayBuffer.empty

  def reset(newKeys: Seq[String], newPageSize: Int = 1000): Unit = {
    keys = newKeys.sorted
    pageSize = newPageSize
    maybeThrowOnListStartingFrom = None
    listStartingFromCalls.clear()
    listStatusCalls.clear()
  }
}
