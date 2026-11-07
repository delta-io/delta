package io.delta.storage

import java.io.{ByteArrayOutputStream, IOException, OutputStream}

import org.scalatest.funsuite.AnyFunSuite

class S3SingleDriverLogStoreSuite extends AnyFunSuite {
  test("the write resource closes when action iteration fails") {
    val output = new TrackingOutputStream
    val actions = new java.util.Iterator[String] {
      override def hasNext: Boolean = true
      override def next(): String = throw new IOException("injected iterator failure")
      override def remove(): Unit = throw new UnsupportedOperationException
    }

    assertThrows[IOException] {
      S3SingleDriverLogStore.writeActions(output, actions)
    }
    assert(output.closed)
  }

  test("interrupted status is restored and the original exception is kept as the cause") {
    val wasInterrupted = Thread.interrupted() // clear and remember any pre-existing flag
    try {
      val original = new InterruptedException("injected interruption")
      val converted = S3SingleDriverLogStore.toInterruptedIOException(original)

      assert(Thread.currentThread().isInterrupted)
      assert(converted.getCause eq original)
      assert(converted.getMessage == original.getMessage)
    } finally {
      Thread.interrupted() // clear the flag this test set
      if (wasInterrupted) Thread.currentThread().interrupt() // restore prior state
    }
  }

  private class TrackingOutputStream extends OutputStream {
    private val delegate = new ByteArrayOutputStream
    var closed = false

    override def write(value: Int): Unit = delegate.write(value)

    override def close(): Unit = {
      closed = true
      delegate.close()
    }
  }
}
