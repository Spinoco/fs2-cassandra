package spinoco.fs2.cassandra.builder

import shapeless.labelled._
import shapeless.ops.hlist.Prepend
import shapeless.ops.record.Selector
import shapeless.{::, HList, HNil, Witness}
import spinoco.fs2.cassandra.internal.TableInstance
import spinoco.fs2.cassandra.{CType, KeySpace, Table}

/**
  * Helper to build type safe definition of the table
  */
case class TableBuilder[R <: HList, PK <: HList, CK <: HList](
  ks:KeySpace
) {

  /** Register supplied name as partitioning key for this table **/
  def partition[K, V](name:Witness.Aux[K])( implicit S: Selector.Aux[R,K,V], P:Prepend[PK,FieldType[K,V] :: HNil])
  :TableBuilder[R, P.Out, CK] = TableBuilder(ks)

  /** Register supplied name as clustering key for this table **/
  def cluster[K,V](name:Witness.Aux[K])( implicit S: Selector.Aux[R,K,V], P:Prepend[CK,FieldType[K,V] :: HNil])
  :TableBuilder[R,  PK, P.Out] = TableBuilder(ks)

  /** registers given `V` as column of this table with name `name` **/
  def column[V](name:Witness)(implicit ev:CType[V])
  : TableBuilder[FieldType[name.T,V] :: R, PK, CK] = TableBuilder(ks)


  def createTable(name:String,options:Map[String,String] = Map.empty)(
    implicit T:TableInstance[R,PK,CK]
  ): Table[R, PK, CK] = T.table(ks,name,options)

}
