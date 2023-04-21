package io.delta.storage

import java.util.function.Supplier

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.s3a.RemoteFileChangedException
import org.scalatest.funsuite.AnyFunSuite

// scalastyle:off println
class RetryableCloseableIteratorSuite extends AnyFunSuite {

  private def getIter(
      range: Range,
      throwAtIndices: Seq[Int] = Seq.empty): CloseableIterator[String] =
    new CloseableIterator[String] {
      var index = 0
      val impl = range.iterator.asJava

      override def close(): Unit = { }

      override def hasNext: Boolean = {
        if (throwAtIndices.contains(index)) {
          println(s"`hasNext` throwing for index $index")
          throw new RemoteFileChangedException(s"path -> index $index", "operation", "msg");
        }

        impl.hasNext
      }

      override def next(): String = {
        if (throwAtIndices.contains(index)) {
          println(s"`next` throwing for index $index")
          throw new RemoteFileChangedException(s"path -> index $index", "operation", "msg");
        }

        index = index + 1

        impl.next().toString
      }
    }

  /**
   * Fails at indices 25, 50, 75, 110.
   *
   * Provide a suitable input range to get the # of failures you want. e.g. range 0 to 100 will fail
   * 3 times.
   */
  def getFailingIterSupplier(range: Range): Supplier[CloseableIterator[String]] =
    new Supplier[CloseableIterator[String]] {
      var numGetCalls = 0

      override def get(): CloseableIterator[String] = numGetCalls match {
        case 0 =>
          // The 1st time this has been called! This will fail at index 25
          numGetCalls = numGetCalls + 1
          getIter(range, Seq(25, 50, 75, 110))
        case 1 =>
          // The 2nd time this has been called! The underlying RetryableCloseableIterator should
          // replay until index 25, and then continue.
          numGetCalls = numGetCalls + 1
          getIter(range, Seq(50, 75, 110))
        case 2 =>
          // The 3rd time this has been called! The underlying RetryableCloseableIterator should
          // replay until index 50, and then continue.
          numGetCalls = numGetCalls + 1
          getIter(range, Seq(75, 110))
        case 3 =>
          // The 4th time this has been called! The underlying RetryableCloseableIterator should
          // replay until index 75, and then continue.
          numGetCalls = numGetCalls + 1
          getIter(range, Seq(110))
        case _ => throw new RuntimeException("Should never all a 5th time - there's only 4 (1st " +
          "call + 3 retries) allowed!")
      }
    }

  test("simple case - internally keeps track of the correct index") {
    val testIter = new RetryableCloseableIterator(() => getIter(0 to 100))
    assert(testIter.getLastSuccessfullIndex == -1)

    for (i <- 0 to 100) {
      val elem = testIter.next()
      assert(elem.toInt == i)
      assert(testIter.getLastSuccessfullIndex == i)
    }

    assert(!testIter.hasNext) // this would be index 101
  }

  test("complex case - replays underlying iter back to correct index after error") {
    // Here, we just do the simplest verification
    val testIter1 = new RetryableCloseableIterator(getFailingIterSupplier(0 to 100))
    val output1 = testIter1.asScala.toList

    // this asserts the size, order, and elements of the testIter1
    assert(output1.map(_.toInt) == (0 to 100).toList)

    // Here, we do more complex verification
    val testIter2 = new RetryableCloseableIterator(getFailingIterSupplier(0 to 100))
    for (_ <- 0 to 24) { testIter2.next() }
    assert(testIter2.getLastSuccessfullIndex == 24)
    assert(testIter2.getNumRetries == 0)

    assert(testIter2.next().toInt == 25) // this will fail once, and then re-scan
    assert(testIter2.getLastSuccessfullIndex == 25)
    assert(testIter2.getNumRetries == 1)

    for (_ <- 26 to 49) { testIter2.next() }
    assert(testIter2.getLastSuccessfullIndex == 49)
    assert(testIter2.getNumRetries == 1)

    assert(testIter2.next().toInt == 50) // this will fail once, and then re-scan
    assert(testIter2.getLastSuccessfullIndex == 50)
    assert(testIter2.getNumRetries == 2)

    for (_ <- 51 to 74) { testIter2.next() }
    assert(testIter2.getLastSuccessfullIndex == 74)
    assert(testIter2.getNumRetries == 2)

    assert(testIter2.next().toInt == 75) // this will fail once, and then re-scan
    assert(testIter2.getLastSuccessfullIndex == 75)
    assert(testIter2.getNumRetries == 3)

    for (_ <- 76 to 100) { testIter2.next() }
    assert(testIter2.getLastSuccessfullIndex == 100)
    assert(!testIter2.hasNext)
  }

  test("throws after MAX_RETRIES exceptions") {
    // Here, we will try to iterate from [0, 200] but getFailingIterSupplier fails at indices
    // 25, 50, 75, 110.
    val testIter = new RetryableCloseableIterator(getFailingIterSupplier(0 to 200))

    for (i <- 0 to 109) {
      assert(testIter.next().toInt == i)
    }
    assert(testIter.getNumRetries == 3)
    val ex = intercept[RuntimeException] {
      testIter.next()
    }
    assert(ex.getCause.isInstanceOf[RemoteFileChangedException])
  }

}
