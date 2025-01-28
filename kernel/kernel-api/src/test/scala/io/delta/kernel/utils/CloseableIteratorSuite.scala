package io.delta.kernel.utils
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.util

class CloseableIteratorSuite extends AnyFunSuite {

  test("test take while with all elements matching filter") {
    val res = new util.ArrayList[Int]();
    new IntArrayIterator(List(1, 2, 3, 2, 2))
      .takeWhile((input: Int) => input > 0)
      .forEachRemaining((input: Int) => res.add(input))
    res should contain theSameElementsAs List(1, 2, 3, 2, 2)
  }

  test("test take while with no elements matching filter") {
    val res = new util.ArrayList[Int]();
    new IntArrayIterator(List(1, 2, 3, 2, 2))
      .takeWhile((input: Int) => input < 0)
      .forEachRemaining((input: Int) => res.add(input))
    assert(res.isEmpty)
  }

  test("test take while with some element matching filter") {
    val res = new util.ArrayList[Int]();
    new IntArrayIterator(List(1, 2, 3, 2, 2))
      .takeWhile((input: Int) => input < 3)
      .forEachRemaining((input: Int) => res.add(input))
    res should contain theSameElementsAs List(1, 2)
  }

}

class IntArrayIterator(val intArray: List[Int]) extends CloseableIterator[Int] {
  val data: List[Int] = intArray
  var curIdx = 0
  override def hasNext: Boolean = {
    data.size > curIdx
  }

  override def next(): Int = {
    val res = data(curIdx)
    curIdx = curIdx + 1
    res
  }
  override def close(): Unit = {}
}
