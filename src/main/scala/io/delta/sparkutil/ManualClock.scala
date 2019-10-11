package io.delta.sparkutil

/**
  * A `Clock` whose time can be manually set and modified. Its reported time does not change
  * as time elapses, but only as its time is modified by callers. This is mainly useful for
  * testing.
  *
  * @param time initial time (in milliseconds since the epoch)
  */
class ManualClock(private var time: Long) extends Clock {

  /**
    * @return `ManualClock` with initial time 0
    */
  def this() = this(0L)

  def getTimeMillis(): Long =
    synchronized {
      time
    }

  /**
    * @param timeToSet new time (in milliseconds) that the clock should represent
    */
  def setTime(timeToSet: Long): Unit = synchronized {
    time = timeToSet
    notifyAll()
  }

  /**
    * @param timeToAdd time (in milliseconds) to add to the clock's time
    */
  def advance(timeToAdd: Long): Unit = synchronized {
    time += timeToAdd
    notifyAll()
  }

  /**
    * @param targetTime block until the clock time is set or advanced to at least this time
    * @return current time reported by the clock when waiting finishes
    */
  def waitTillTime(targetTime: Long): Long = synchronized {
    while (time < targetTime) {
      wait(10)
    }
    getTimeMillis()
  }
}
