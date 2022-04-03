package io.delta.storage

import java.io.IOException

import scala.util.Random

import org.apache.hadoop.fs.FileAlreadyExistsException
import org.scalatest.funsuite.AnyFunSuite

class ThreadUtilsSuite extends AnyFunSuite {
  test("runInNewThread") {
    import io.delta.storage.internal.ThreadUtils.runInNewThread

    assert(runInNewThread("thread-name",
      true,
      () => { Thread.currentThread().getName}) === "thread-name"
    )
    assert(runInNewThread("thread-name",
      true,
      () => { Thread.currentThread().isDaemon })
    )
    assert(runInNewThread("thread-name",
      false,
      () => { Thread.currentThread().isDaemon } === false)
    )

    def verifyException(exception: Exception, expectedMessage: String): Unit = {
      assert(exception.getMessage === expectedMessage)
      assert(exception.getStackTrace.mkString("\n")
        .contains("... run in separate thread using ThreadUtils"))
      assert(!exception.getStackTrace.mkString("\n").contains("ThreadUtils.java"))
    }

    // Exception case 1: IOException
    val ioExceptionMessage = "test" + Random.nextInt()
    val ioException = intercept[IOException] {
      runInNewThread("thread-name",
        true,
        () => {
          throw new IOException(ioExceptionMessage)
        })
    }
    verifyException(ioException, ioExceptionMessage)

    // Exception case 2: org.apache.hadoop.fs.FileAlreadyExistsException
    val fileAlreadyExistsExceptionMessage = "test" + Random.nextInt()
    val fileAlreadyExistsException = intercept[FileAlreadyExistsException] {
      runInNewThread("thread-name",
        true,
        () => {
          throw new FileAlreadyExistsException(fileAlreadyExistsExceptionMessage)
        })
    }
    verifyException(fileAlreadyExistsException, fileAlreadyExistsExceptionMessage)

    // Exception case 3: Any other exception should be re-thrown as a RuntimeException
    val otherExceptionMessage = "test" + Random.nextInt()
    val otherException = intercept[RuntimeException] {
      runInNewThread("thread-name",
        true,
        () => {
          throw new Exception(otherExceptionMessage)
        })
    }
    verifyException(otherException, otherExceptionMessage)
  }
}
