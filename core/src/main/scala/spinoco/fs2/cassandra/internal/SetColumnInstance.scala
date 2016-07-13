package spinoco.fs2.cassandra.internal

import spinoco.fs2.cassandra.CType

/**
  * Instance guarding column type of `Set`
  */
trait SetColumnInstance[C[_],V]


object SetColumnInstance  {


  implicit def setInstance[V : CType]: SetColumnInstance[Set,V] =
    new SetColumnInstance[Set,V] {}

}



