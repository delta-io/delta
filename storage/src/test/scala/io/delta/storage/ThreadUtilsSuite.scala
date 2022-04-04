package io.delta.storage

import java.io.IOException

import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite

class ThreadUtilsSuite extends AnyFunSuite {
  test("runInNewThread") {
    import io.delta.storage.internal.ThreadUtils.runInNewThread

    assert(runInNewThread("thread-name",
      true,
      () => {
        Thread.currentThread().getName
      }) === "thread-name"
    )
    assert(runInNewThread("thread-name",
      true,
      () => {
        Thread.currentThread().isDaemon
      })
    )
    assert(runInNewThread("thread-name",
      false,
      () => {
        Thread.currentThread().isDaemon
      } === false)
    )

    val ioExceptionMessage = "test" + Random.nextInt()
    val ioException = intercept[IOException] {
      runInNewThread("thread-name",
        true,
        () => {
          throw new IOException(ioExceptionMessage)
        })
    }
    assert(ioException.getMessage === ioExceptionMessage)
    assert(ioException.getStackTrace.mkString("\n")
      .contains("... run in separate thread using ThreadUtils"))
    assert(!ioException.getStackTrace.mkString("\n").contains("ThreadUtils.java"))
  }
}
