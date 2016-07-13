package spinoco.fs2.cassandra.internal

import shapeless.labelled.FieldType
import shapeless.ops.record.Selector
import shapeless.{::, DepFn1, HList, HNil}

/**
  * Type class supporting multiple record field selection.
  *
  * @author Miles Sabin
  */
/* moved from shapeless as the current shapeless version does not seem to work properly **/
@annotation.implicitNotFound(msg = "No fields ${K} in record ${L}")
trait SelectAll[L <: HList, K <: HList] extends DepFn1[L] with Serializable { type Out <: HList }

object SelectAll {
  def apply[L <: HList, K <: HList](implicit sa: SelectAll[L, K]): Aux[L, K, sa.Out] = sa

  type Aux[L <: HList, K <: HList, Out0 <: HList] = SelectAll[L, K] { type Out = Out0 }

  implicit def hnilSelectAll[L <: HList]: Aux[L, HNil, HNil] =
    new SelectAll[L, HNil] {
      type Out = HNil
      def apply(l: L): Out = HNil
    }

  implicit def hconsSelectAll[L <: HList, K,V, KT <: HList]
  (implicit
   sh: Selector.Aux[L, K,V],
   st: SelectAll[L, KT]
  ): Aux[L, FieldType[K,V] :: KT, FieldType[K,V] :: st.Out] =
    new SelectAll[L, FieldType[K,V] :: KT] {
      type Out = FieldType[K,V] :: st.Out
      def apply(l: L): Out = sh(l).asInstanceOf[FieldType[K,V]] :: st(l)
    }
}
