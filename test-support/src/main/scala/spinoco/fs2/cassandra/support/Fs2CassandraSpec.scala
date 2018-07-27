package spinoco.fs2.cassandra.support

import org.scalatest.concurrent.{Eventually, TimeLimitedTests}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{FreeSpec, Matchers}

import scala.concurrent.ExecutionContext


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


  implicit val EC: ExecutionContext = spinoco.fs2.cassandra.support.EC
//  implicit val Sch: Scheduler =  spinoco.fs2.cassandra.support.Sch


}


