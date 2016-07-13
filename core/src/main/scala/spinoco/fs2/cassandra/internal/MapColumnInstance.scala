package spinoco.fs2.cassandra.internal

import spinoco.fs2.cassandra.{CType, MapKeyCType}

/**
  * Created by pach on 11/06/16.
  */
trait MapColumnInstance[M <: Map[_,_]] {
  type K
  type V
}


object MapColumnInstance {
  type Aux[M <: Map[_,_], K0,V0] = MapColumnInstance[M] { type K = K0; type V = V0 }


  implicit def mapInstance[K0 :MapKeyCType ,V0 :CType]:MapColumnInstance.Aux[Map[K0,V0],K0,V0] =
    new MapColumnInstance[Map[K0,V0]] { type K = K0; type V = V0}

}