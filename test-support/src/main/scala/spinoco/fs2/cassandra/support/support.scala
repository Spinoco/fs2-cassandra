package spinoco.fs2.cassandra

import java.util.concurrent.{Executors, ThreadFactory}

import cats.effect.{ContextShift, IO}

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

  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8, factory))
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)

}
