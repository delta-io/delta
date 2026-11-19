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
package org.apache.spark.sql.delta.v2.interop

import java.io.{InterruptedIOException, IOException, UncheckedIOException}
import java.nio.channels.ClosedByInterruptException
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.storage.{ClosableIterator, HDFSLogStore}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import io.delta.kernel.{CommitFileStatus => KernelCommitFileStatus}
import io.delta.kernel.utils.{FileStatus => KernelFileStatus}

import org.apache.spark.{SparkConf, SparkFunSuite}

class DeltaV2SingleCommitSuite extends SparkFunSuite {

  private val actionJsons = Seq(
    """{"add":{"path":"added.parquet","partitionValues":{},"size":1,
      |"modificationTime":1,"dataChange":true}}""".stripMargin,
    """{"remove":{"path":"removed.parquet","deletionTimestamp":2,"dataChange":true}}""",
    """{"metaData":{"id":"table-id","format":{"provider":"parquet","options":{}},
      |"schemaString":"{\"type\":\"struct\",\"fields\":[]}",
      |"partitionColumns":[],"configuration":{}}}""".stripMargin)
  private val expectedActions = actionJsons.map(Action.fromJson).toList

  private class TrackingLogStore(conf: Configuration)
      extends HDFSLogStore(new SparkConf(false), conf) {
    var opens = 0
    var closes = 0
    var onOperation: String => Unit = (_: String) => ()

    override def readAsIterator(
        fileStatus: FileStatus,
        hadoopConf: Configuration): ClosableIterator[String] = {
      assert(hadoopConf eq conf)
      onOperation("open")
      opens += 1
      val underlying = super.readAsIterator(fileStatus, hadoopConf)
      new ClosableIterator[String] {
        private var closed = false
        override def hasNext: Boolean = {
          onOperation("hasNext")
          underlying.hasNext
        }
        override def next(): String = {
          onOperation("next")
          underlying.next()
        }
        override def close(): Unit = {
          if (!closed) {
            closed = true
            closes += 1
            underlying.close()
            onOperation("close")
          }
        }
      }
    }
  }

  private def withCommit(jsons: Seq[String] = actionJsons)(
      f: (DeltaV2SingleCommit, TrackingLogStore, Long) => Unit): Unit = {
    withTempDir { dir =>
      val file = dir.toPath.resolve("commit with spaces.json")
      Files.write(file, (jsons.map(_.replace("\n", "")).mkString("\n") + "\n").getBytes(UTF_8))
      val fileSize = Files.size(file)
      val conf = new Configuration()
      val store = new TrackingLogStore(conf)
      val status = new KernelCommitFileStatus(
        7L, KernelFileStatus.of(file.toString, fileSize, 3000L))
      val commit = DeltaV2SingleCommit(status, store, conf)
      f(commit, store, fileSize)
    }
  }

  Seq(-1L, 0L, 1L, Long.MaxValue).foreach { threshold =>
    test(s"actions rewind after partial consumption and exhaustion (threshold=$threshold)") {
      withCommit() { (commit, store, size) =>
        val actions = commit.getActionsIterator(threshold)
        val cached = size < threshold
        try {
          assert(store.opens === 1)
          assert(store.closes === (if (cached) 1 else 0))
          assert(actions.next() === expectedActions.head)
          actions.rewind()
          assert(actions.toList === expectedActions)
          assert(!actions.hasNext)
          intercept[NoSuchElementException] { actions.next() }
          actions.rewind()
          assert(actions.toList === expectedActions)
          assert(store.opens === (if (cached) 1 else 3))
        } finally {
          actions.close()
        }
        assert(store.closes === store.opens)
      }
    }
  }

  test("files equal to the memory threshold stream and smaller files are cached") {
    Seq(false, true).foreach { cached =>
      withCommit() { (commit, store, size) =>
        val actions = commit.getActionsIterator(size + (if (cached) 1L else 0L))
        try {
          assert(actions.toList === expectedActions)
          actions.rewind()
          assert(actions.toList === expectedActions)
          assert(store.opens === (if (cached) 1 else 2))
        } finally {
          actions.close()
        }
        assert(store.closes === store.opens)
      }
    }
  }

  Seq(false, true).foreach { closedByInterrupt =>
    Seq("open", "read", "rewind", "close").foreach { operation =>
      test(s"interrupted $operation retains its cause (closedByInterrupt=$closedByInterrupt)") {
        withCommit() { (commit, store, _) =>
          val interruption = if (closedByInterrupt) {
            new ClosedByInterruptException()
          } else {
            new InterruptedIOException("log read interrupted")
          }
          val actions = if (operation == "rewind" || operation == "close") {
            Some(commit.getActionsIterator())
          } else {
            None
          }
          val failedOperation = operation match {
            case "read" => "hasNext"
            case "rewind" => "open"
            case other => other
          }
          store.onOperation = current => if (current == failedOperation) throw interruption

          try {
            val error = intercept[UncheckedIOException] {
              operation match {
                case "open" => commit.getActionsIterator()
                case "read" => commit.getActionsIterator(Long.MaxValue)
                case "rewind" => actions.get.rewind()
                case "close" => actions.get.close()
                case other => fail(s"Unexpected operation: $other")
              }
            }
            assert(error.getCause eq interruption)
          } finally {
            actions.foreach(_.close())
          }
          assert(store.closes === store.opens)
        }
      }
    }
  }

  test("ordinary read failures keep their original exception and close the reader") {
    withCommit() { (commit, store, _) =>
      val failure = new IOException("log read failed")
      store.onOperation = operation => if (operation == "hasNext") throw failure
      val error = intercept[IOException] {
        commit.getActionsIterator().processAndClose(_.hasNext)
      }
      assert(error eq failure)
      assert(store.opens === 1)
      assert(store.closes === 1)
    }
  }

  Seq(None, Some(1234L)).foreach { ict =>
    test(s"timestamp lazily uses persisted ICT or file modification time (ICT=$ict)") {
      val jsons = ict.toSeq.map(ts => s"""{"commitInfo":{"inCommitTimestamp":$ts}}""") ++
        actionJsons
      withCommit(jsons) { (commit, store, _) =>
        assert(store.opens === 0)
        assert(commit.timestamp === ict.getOrElse(3000L))
        assert(store.opens === 1)
        assert(store.closes === 1)
        assert(commit.timestamp === ict.getOrElse(3000L))
        assert(store.opens === 1)
      }
    }
  }
}
