package spinoco.fs2.cassandra.support

import org.scalatest.concurrent.{Eventually, TimeLimitedTests}
import org.scalatest.matchers.should.Matchers
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.ExecutionContext


/**
  * Created by pach on 07/06/16.
  */
class Fs2CassandraSpec extends AnyFreeSpec
  with ScalaCheckPropertyChecks
  with Matchers
  with TimeLimitedTests
  with Eventually {

  implicit val ioRuntimeGlobal: cats.effect.unsafe.IORuntime = cats.effect.unsafe.implicits.global

  val timeLimit = Span(90, Seconds)

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = timeLimit)

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 25)


  implicit val EC: ExecutionContext = spinoco.fs2.cassandra.support.EC


}


