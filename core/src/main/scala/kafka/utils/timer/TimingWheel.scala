package kafka.utils.timer

import kafka.utils.nonthreadsafe

import java.util.concurrent.DelayQueue
import java.util.concurrent.atomic.AtomicInteger

/**
 * 分层时间轮：Hierarchical Timing Wheels
 */
@nonthreadsafe
private[timer] class TimingWheel(tickMs: Long, wheelSize: Int, startMs: Long, taskCounter: AtomicInteger, queue: DelayQueue[TimerTaskList]) {

  /** 时间周期、槽位数组、当前时间戳、该时间轮的上层时间轮 */
  private[this] val interval = tickMs * wheelSize
  private[this] val buckets = Array.tabulate[TimerTaskList](wheelSize) { _ => new TimerTaskList(taskCounter) }
  private[this] var currentTime = startMs - (startMs % tickMs) // rounding down to multiple of tickMs
  @volatile private[this] var overflowWheel: TimingWheel = null

  private[this] def addOverflowWheel(): Unit = {
    synchronized {
      if (overflowWheel == null) {
        overflowWheel = new TimingWheel(
          tickMs = interval,
          wheelSize = wheelSize,
          startMs = currentTime,
          taskCounter = taskCounter,
          queue
        )
      }
    }
  }

  /** 添加任务 */
  def add(timerTaskEntry: TimerTaskEntry): Boolean = {
    val expiration = timerTaskEntry.expirationMs
    if (timerTaskEntry.cancelled) {
      // #1 已取消
      false
    } else if (expiration < currentTime + tickMs) {
      // #2 已到期
      false
    } else if (expiration < currentTime + interval) {
      // #3 在本层的时间周期内：Put in its own bucket，计算槽位、添加任务到该槽位的双向链表中、更新槽过期时间（若改变则添加该槽到延迟队列中）
      val virtualId = expiration / tickMs
      val bucket = buckets((virtualId % wheelSize.toLong).toInt)
      bucket.add(timerTaskEntry)
      // Set the bucket expiration time
      if (bucket.setExpiration(virtualId * tickMs)) {
        // The bucket needs to be enqueued because it was an expired bucket
        // We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel has advanced
        // and the previous buckets gets reused; further calls to set the expiration within the same wheel cycle
        // will pass in the same value and hence return false, thus the bucket with the same expiration will not
        // be enqueued multiple times.
        queue.offer(bucket)
      }
      true
    } else {
      // #4 超过本层的时间周期：Out of the interval. Put it into the parent timer，添加该任务到该层的上层时间轮中
      if (overflowWheel == null) addOverflowWheel()
      overflowWheel.add(timerTaskEntry)
    }
  }

  // Try to advance the clock
  def advanceClock(timeMs: Long): Unit = {
    if (timeMs >= currentTime + tickMs) {
      currentTime = timeMs - (timeMs % tickMs)
      // Try to advance the clock of the overflow wheel if present
      if (overflowWheel != null) overflowWheel.advanceClock(currentTime)
    }
  }
}
