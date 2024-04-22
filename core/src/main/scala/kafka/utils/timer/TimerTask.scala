package kafka.utils.timer

trait TimerTask extends Runnable {

  /** 延时时间戳、关联的TimerTaskEntry */
  val delayMs: Long
  private[this] var timerTaskEntry: TimerTaskEntry = null

  def cancel(): Unit = {
    synchronized {
      if (timerTaskEntry != null) timerTaskEntry.remove()
      timerTaskEntry = null
    }
  }

  private[timer] def setTimerTaskEntry(entry: TimerTaskEntry): Unit = {
    synchronized {
      if (timerTaskEntry != null && timerTaskEntry != entry)
        timerTaskEntry.remove()
      timerTaskEntry = entry
    }
  }

  private[timer] def getTimerTaskEntry: TimerTaskEntry = timerTaskEntry

}
