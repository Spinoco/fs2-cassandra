package spinoco.fs2.cassandra

import shapeless.tag.@@
import spinoco.fs2.cassandra.ctype.CType
import spinoco.fs2.cassandra.ctype.CType.{TTL, Type1}

import java.time.LocalDateTime
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

object functions {
  val count: CQLFunction0[Int] = CQLFunction0("count(*)")

  val dateOf: CQLFunction[UUID @@ Type1, LocalDateTime] = CQLFunction(name => s"dateOf($name)")
  val unixTimestampOf: CQLFunction[UUID @@ Type1, Long] = CQLFunction(name => s"unixTimestampOf($name)")

  def writeTimeOfMicro[I : CType] : CQLFunction[I, Long] = CQLFunction(name => s"WRITETIME($name)")
  def ttlOf[I: CType] : CQLFunction[I, Option[FiniteDuration @@ TTL]] = CQLFunction(name => s"TTL($name)")

  def similarityCosine[A: CType, T]: CQLFunction2[Vector[A] @@ T, Vector[A] @@ T, Float] =
    CQLFunction2((col, param) => s"similarity_cosine($col, :$param)")

  def similarityEuclidean[A: CType, T]: CQLFunction2[Vector[A] @@ T, Vector[A] @@ T, Float] =
    CQLFunction2((col, param) => s"similarity_euclidean($col, :$param)")

  def similarityDotProduct[A: CType, T]: CQLFunction2[Vector[A] @@ T, Vector[A] @@ T, Float] =
    CQLFunction2((col, param) => s"similarity_dot_product($col, :$param)")
}

/** cql function taking column as parameter **/
trait CQLFunction[I, O] {
  def apply(column: String): String
}

/** CQl function w/o column as parameter **/
trait CQLFunction0[O] {
  def apply(): String
}

/** CQL function taking a column and a bound parameter **/
trait CQLFunction2[I1, I2, O] {
  def apply(column: String, param: String): String
}

object CQLFunction0 {
  def apply[O](s: String): CQLFunction0[O] = new CQLFunction0[O] {
    def apply(): String = s
  }
}

object CQLFunction {

  def apply[I: CType, O: CType](f: String => String): CQLFunction[I, O] =
    new CQLFunction[I, O] { def apply(s: String): String = f(s) }

}

object CQLFunction2 {

  def apply[I1, I2, O](f: (String, String) => String): CQLFunction2[I1, I2, O] =
    new CQLFunction2[I1, I2, O] { def apply(c: String, p: String): String = f(c, p) }

}
