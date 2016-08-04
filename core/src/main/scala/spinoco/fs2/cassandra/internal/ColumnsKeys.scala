package spinoco.fs2.cassandra.internal

import shapeless.HList
import shapeless.ops.hlist.ToTraversable
import shapeless.ops.record.Keys
import spinoco.fs2.cassandra.internal

/**
  * Created by adamchlupacek on 03/08/16.
  */
trait ColumnsKeys[C <: HList]{
  def keys: Seq[String]
}

object ColumnsKeys {

  implicit def instance[C <: HList, KL <: HList](
    implicit K: Keys.Aux[C, KL]
    , trav: ToTraversable.Aux[KL, List, Any]
  ):ColumnsKeys[C] = {
    new ColumnsKeys[C] {
      def keys: Seq[String] = K().toList.map(internal.asKeyName)
    }
  }
}
