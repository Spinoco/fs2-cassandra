package spinoco.fs2.cassandra.internal

import spinoco.fs2.cassandra.CType

/**
  * Created by pach on 11/06/16.
  */
trait ListColumnInstance[C[_],V]


object ListColumnInstance {


  implicit def seqInstance[V](implicit ev:CType[V]):ListColumnInstance[Seq,V] =
    new ListColumnInstance[Seq,V] { }

  implicit def listInstance[V](implicit ev:CType[V]):ListColumnInstance[List,V] =
    new ListColumnInstance[List,V] { }

  implicit def vectorInstance[V](implicit ev:CType[V]):ListColumnInstance[Vector,V] =
    new ListColumnInstance[Vector,V] { }

}
