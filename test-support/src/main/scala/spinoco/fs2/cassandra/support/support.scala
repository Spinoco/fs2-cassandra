package spinoco.fs2.cassandra

import java.util.concurrent.{Executors, ThreadFactory}

import fs2.Scheduler

import scala.concurrent.ExecutionContext

/**
  * Created by pach on 09/06/16.
  */
package object support {

  val factory: ThreadFactory = new ThreadFactory {
    def newThread(r: Runnable): Thread = {
      val thread = new Thread(r)
      thread.setDaemon(true)
      thread.setName("fs2-c8-spec")
      thread
    }
  }

  implicit val EC: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8, factory))
  implicit val Sch: Scheduler =  Scheduler.fromScheduledExecutorService(Executors.newScheduledThreadPool(4, factory))

}
