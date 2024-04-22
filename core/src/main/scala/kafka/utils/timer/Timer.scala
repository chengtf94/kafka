package kafka.utils.timer

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{DelayQueue, Executors, TimeUnit}

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.{KafkaThread, Time}

trait Timer {

  def add(timerTask: TimerTask): Unit

  def advanceClock(timeoutMs: Long): Boolean

  def size: Int

  def shutdown(): Unit

}

@threadsafe
class SystemTimer(executorName: String,
                  tickMs: Long = 1,
                  wheelSize: Int = 20,
                  startMs: Long = Time.SYSTEM.hiResClockMs) extends Timer {

  /** 线程池、延迟队列、任务计数器、时间轮 */
  private[this] val taskExecutor = Executors.newFixedThreadPool(1,
    (runnable: Runnable) => KafkaThread.nonDaemon("executor-" + executorName, runnable))
  private[this] val delayQueue = new DelayQueue[TimerTaskList]()
  private[this] val taskCounter = new AtomicInteger(0)
  private[this] val timingWheel = new TimingWheel(
    tickMs = tickMs,
    wheelSize = wheelSize,
    startMs = startMs,
    taskCounter = taskCounter,
    delayQueue
  )

  /** 锁：当ticking时保护数据结构 */
  // Locks used to protect data structures while ticking
  private[this] val readWriteLock = new ReentrantReadWriteLock()
  private[this] val readLock = readWriteLock.readLock()
  private[this] val writeLock = readWriteLock.writeLock()

  @Override
  def add(timerTask: TimerTask): Unit = {
    readLock.lock()
    try {
      addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs))
    } finally {
      readLock.unlock()
    }
  }

  private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
    if (!timingWheel.add(timerTaskEntry)) {
      if (!timerTaskEntry.cancelled) {
        // 任务已到期且未取消，则执行该任务
        taskExecutor.submit(timerTaskEntry.timerTask)
      }
    }
  }

  @Override
  def advanceClock(timeoutMs: Long): Boolean = {
    // 从延迟队列获取槽，阻塞等待直到timeoutMs超时或堆顶任务到期
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    if (bucket != null) {
      writeLock.lock()
      try {
        while (bucket != null) {
          // 更新每层时间轮的currentTime
          timingWheel.advanceClock(bucket.getExpiration)
          // 重新进行任务插入，以实现时间轮的降级
          bucket.flush(addTimerTaskEntry)
          // 获取下一个槽
          bucket = delayQueue.poll()
        }
      } finally {
        writeLock.unlock()
      }
      true
    } else {
      false
    }
  }

  @Override
  def size: Int = taskCounter.get

  @Override
  override def shutdown(): Unit = {
    taskExecutor.shutdown()
  }

}
