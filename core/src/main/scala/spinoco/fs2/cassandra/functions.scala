package spinoco.fs2.cassandra

import java.time.LocalDateTime
import java.util.UUID

import shapeless.tag.@@
import spinoco.fs2.cassandra.CType.{TTL, Type1}

import scala.concurrent.duration.FiniteDuration


object functions {
  val count:CQLFunction0[Int] = CQLFunction0("count(*)")

  val dateOf: CQLFunction[UUID @@ Type1, LocalDateTime] = CQLFunction(name => s"dateOf($name)")
  val unixTimestampOf: CQLFunction[UUID @@ Type1,Long] = CQLFunction(name => s"unixTimestampOf($name)")

  def writeTimeOfMicro[I : CType] : CQLFunction[I,Long] = CQLFunction(name => s"WRITETIME($name)")
  def ttlOf[I : CType] : CQLFunction[I,Option[FiniteDuration @@ TTL]] = CQLFunction(name => s"TTL($name)")
}

/** cql function taking column as parameter **/
trait CQLFunction[I,O] {
  def apply(column:String):String
}

/** CQl function w/o column as parameter **/
trait CQLFunction0[O] {
  def apply():String
}

object CQLFunction0 {
  def apply[O](s:String):CQLFunction0[O] = new CQLFunction0[O] {
    def apply(): String = s
  }
}

object CQLFunction {

  def apply[I :CType,O :CType](f: String => String): CQLFunction[I,O] =
    new CQLFunction[I,O] { def apply(s:String):String = f(s) }

}
