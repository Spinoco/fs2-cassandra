package spinoco.fs2.cassandra

import fs2.{Scheduler, Strategy}

/**
  * Created by pach on 09/06/16.
  */
package object support {


  implicit val S: Strategy = Strategy.fromFixedDaemonPool(8,"fs2-c8-spec")
  implicit val Sch: Scheduler =  Scheduler.fromFixedDaemonPool(4, "fs2-c8-spec-scheduler")

}
