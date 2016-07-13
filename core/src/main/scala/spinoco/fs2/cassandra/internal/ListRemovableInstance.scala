package spinoco.fs2.cassandra.internal

import spinoco.fs2.cassandra.CType


trait ListRemovableInstance[C[_],V]

object ListRemovableInstance {

  implicit def setInstance[V : CType]: ListRemovableInstance[Set,V] =
    new ListRemovableInstance[Set,V] {}

  implicit def seqInstance[V](implicit ev:CType[V]):ListRemovableInstance[Seq,V] =
    new ListRemovableInstance[Seq,V] { }

  implicit def listInstance[V](implicit ev:CType[V]):ListRemovableInstance[List,V] =
    new ListRemovableInstance[List,V] { }

  implicit def vectorInstance[V](implicit ev:CType[V]):ListRemovableInstance[Vector,V] =
    new ListRemovableInstance[Vector,V] { }

}
