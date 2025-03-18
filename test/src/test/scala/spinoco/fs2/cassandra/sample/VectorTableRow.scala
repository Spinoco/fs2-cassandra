package spinoco.fs2.cassandra.sample

import shapeless.tag
import shapeless.tag.@@
import spinoco.fs2.cassandra.ctype.CType
import spinoco.fs2.cassandra.ctype.types.VectorCType
import spinoco.fs2.cassandra.sample.VectorSizes.{VectorSize4, VectorSize8}


object VectorSizes {
  trait VectorSize4
  trait VectorSize8
  implicit def vectorInstance4[A: CType : Numeric]: CType[Vector[A] @@ VectorSize4] = VectorCType.instance[A, VectorSize4](4)
  implicit def vectorInstance8[A: CType : Numeric]: CType[Vector[A] @@ VectorSize8] = VectorCType.instance[A, VectorSize8](8)
}


case class VectorTableRow(
   intColumn: Int
   , longColumn: Long
   , vector4IntColumn: Vector[Int] @@ VectorSize4
   , vector8FloatColumn: Vector[Float] @@ VectorSize8
 )


object VectorTableRow {
  val instance: VectorTableRow = VectorTableRow(
    intColumn = 1
    , longColumn = 1
    , vector4IntColumn = tag[VectorSize4](Vector(5,6,7,8))
    , vector8FloatColumn = tag[VectorSize8](Vector(1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f))
  )
}
