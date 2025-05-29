package io.delta.kernel
import java.util.{Iterator => JIterator, NoSuchElementException}

import scala.collection.JavaConverters.seqAsJavaListConverter

import io.delta.kernel.utils.PeekableIterator

import org.scalatest.funsuite.AnyFunSuite

class PeekableIteratorSuite extends AnyFunSuite {

  test("peek should return the next element without consuming it") {
    val list = List(1, 2, 3).asJava
    val peekable = new PeekableIterator(list.iterator())

    assert(peekable.peek() === 1)
    assert(peekable.peek() === 1) // Should return same element
    assert(peekable.hasNext === true)
    assert(peekable.next() === 1) // Should return the peeked element
  }

  test("peek should throw NoSuchElementException when no elements available") {
    val emptyList = List.empty[Int].asJava
    val peekable = new PeekableIterator(emptyList.iterator())

    assertThrows[NoSuchElementException] {
      peekable.peek()
    }
  }

  test("peek and next should work correctly ") {
    val list = List(1, 2).asJava
    val peekable = new PeekableIterator(list.iterator())

    assert(peekable.hasNext === true)
    assert(peekable.peek() === 1)
    assert(peekable.next() === 1)

    assert(peekable.hasNext === true)
    assert(peekable.peek() === 2)
    assert(peekable.next() === 2)

    assert(peekable.hasNext === false)
  }
}
