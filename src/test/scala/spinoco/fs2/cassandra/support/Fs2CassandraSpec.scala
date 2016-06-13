package spinoco.fs2.cassandra.support

import fs2.{Scheduler, Strategy}
import org.scalatest.concurrent.{Eventually, TimeLimitedTests}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{FreeSpec, Matchers}


/**
  * Created by pach on 07/06/16.
  */
class Fs2CassandraSpec extends FreeSpec
  with GeneratorDrivenPropertyChecks
  with Matchers
  with TimeLimitedTests
  with Eventually {


  val timeLimit = Span(90, Seconds)

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = timeLimit)

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 25, workers = 1)


  implicit val S: Strategy = spinoco.fs2.cassandra.support.S
  implicit val Sch: Scheduler =  spinoco.fs2.cassandra.support.Sch


}


