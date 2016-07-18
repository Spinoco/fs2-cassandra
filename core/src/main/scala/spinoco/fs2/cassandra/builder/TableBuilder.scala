package spinoco.fs2.cassandra.builder

import shapeless.labelled._
import shapeless.ops.hlist.Prepend
import shapeless.ops.record.Selector
import shapeless.{::, HList, HNil, Witness}
import spinoco.fs2.cassandra.internal
import spinoco.fs2.cassandra.internal.TableInstance
import spinoco.fs2.cassandra.{CType, KeySpace, Table}

/**
  * Helper to build type safe definition of the table
  */
case class TableBuilder[R <: HList, PK <: HList, CK <: HList, IDX <: HList](
  ks:KeySpace
  , indexes: Seq[IndexEntry]
  , partitionKeys: Seq[String]
  , clusterKeys: Seq[String]
) { self =>

  /** Register supplied name as partitioning key for this table **/
  def partition[K, V](name:Witness.Aux[K])( implicit S: Selector.Aux[R,K,V], P:Prepend[PK,FieldType[K,V] :: HNil])
  :TableBuilder[R, P.Out, CK, IDX] = TableBuilder(ks, indexes,  partitionKeys :+ internal.keyOf(name), clusterKeys)

  /** Register supplied name as clustering key for this table **/
  def cluster[K,V](name:Witness.Aux[K])( implicit S: Selector.Aux[R,K,V], P:Prepend[CK,FieldType[K,V] :: HNil])
  :TableBuilder[R,  PK, P.Out, IDX] = TableBuilder(ks, indexes, partitionKeys, clusterKeys :+  internal.keyOf(name) )

  /** registers given `V` as column of this table with name `name` **/
  def column[K,V](name:Witness.Aux[K])(implicit ev:CType[V])
  : TableBuilder[FieldType[K,V] :: R, PK, CK, IDX] = TableBuilder(ks, indexes, partitionKeys, clusterKeys)

  /** creates secondary index on specified table column **/
  def indexBy[K,V](column:Witness.Aux[K], name:String, clazz:Option[String] = None, options:Map[String, String] = Map.empty)(
    implicit S: Selector.Aux[R,K,V]
  ): TableBuilder[R,PK,CK,FieldType[K,V] :: IDX] = {
    TableBuilder(ks, IndexEntry(name,internal.keyOf(column),clazz,options) +: indexes, partitionKeys, clusterKeys )
  }

  /** create secondary SASI index of Prefix type **/
  def indexByPrefix[K,V](column:Witness.Aux[K], name:String, options:Map[String, String] = Map.empty)(
    implicit S: Selector.Aux[R,K,V]
  ): TableBuilder[R,PK,CK,FieldType[K,V] :: IDX] = {
    indexBy(column, name, Some(IndexEntry.SASIIndexClz), options )
  }

  /** create secondary SASI index of Contains type **/
  def indexByContains[K,V](column:Witness.Aux[K], name:String, clazz:Option[String] = None, options:Map[String, String] = Map.empty)(
    implicit S: Selector.Aux[R,K,V]
  ): TableBuilder[R,PK,CK,FieldType[K,V] :: IDX] =
  indexBy(column, name, Some(IndexEntry.SASIIndexClz), Map("mode" -> "CONTAINS") ++ options )


  /** create secondary SASI index of Sparse type **/
  def indexBySparse[K,V](column:Witness.Aux[K], name:String, clazz:Option[String] = None, options:Map[String, String] = Map.empty)(
    implicit S: Selector.Aux[R,K,V]
  ): TableBuilder[R,PK,CK,FieldType[K,V] :: IDX] =
  indexBy(column, name, Some(IndexEntry.SASIIndexClz), Map("mode" -> "SPARSE") ++ options )


  def build(name:String, options:Map[String,String] = Map.empty)(
    implicit T:TableInstance[R,PK,CK, IDX]
  ): Table[R, PK, CK, IDX] = T.table(ks,name,options, self.indexes, self.partitionKeys, self.clusterKeys)

}
