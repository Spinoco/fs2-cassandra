package spinoco.fs2.cassandra

import shapeless.HNil
import spinoco.fs2.cassandra.internal.ctype.HListCType
import spinoco.fs2.cassandra.support.DockerCassandra


trait CommonCassandraSpec
  extends UpdateSpec
    with InsertSpec
    with SchemaSpec
    with CrudSpec
    with DeleteSpec
    with PagingSpec
    with BatchSpec
    with QuerySpec
    with MigrationsSpec
    with DockerCassandra



object Foo {
  import shapeless.::
  type HH = Int :: String :: HNil

  val hlistInstance: HListCType[HH] =
    HListCType.hlistInstance

}