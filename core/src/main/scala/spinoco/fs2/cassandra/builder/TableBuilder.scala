package spinoco.fs2.cassandra.builder

import shapeless.labelled._
import shapeless.ops.hlist.Prepend
import shapeless.ops.record.Selector
import shapeless.{::, HList, HNil, Witness}
import spinoco.fs2.cassandra.ctype.CType
import spinoco.fs2.cassandra.internal.TableInstance
import spinoco.fs2.cassandra.macros.CTypeRecord
import spinoco.fs2.cassandra.{KeySpace, Table, internal}

/**
  * Helper to build type safe definition of the table
  *
  * @param descendingKeys cluster keys stored in descending order, see `cluster`
  */
case class TableBuilder[R <: HList, PK <: HList, CK <: HList, IDX <: HList](
  ks: KeySpace
  , indexes: Seq[IndexEntry]
  , partitionKeys: Seq[String]
  , clusterKeys: Seq[String]
  , descendingKeys: Set[String] = Set.empty
) { self =>

  /** Register supplied name as partitioning key for this table **/
  def partition[K, V](name: Witness.Aux[K])(
    implicit S: Selector.Aux[R,K,V], P: Prepend[PK,FieldType[K,V] :: HNil]
  ): TableBuilder[R, P.Out, CK, IDX] = TableBuilder(ks, indexes,  partitionKeys :+ internal.keyOf(name), clusterKeys, descendingKeys)

  /**
    * Register supplied name as clustering key for this table.
    *
    * Rows of a partition are stored, and returned when a query has no `ORDER BY`, in the order of the cluster keys.
    * A query cannot have `ORDER BY` together with a secondary index (SAI included), so such queries always return
    * rows in this order. The order cannot be altered once the table exists: migration drops and creates the table
    * when it changes, as when the primary key changes.
    *
    * @param ascending whether the rows are stored in ascending (default) or descending order of this key
    */
  def cluster[K, V](name: Witness.Aux[K], ascending: Boolean = true)(
    implicit S: Selector.Aux[R, K, V], P: Prepend[CK, FieldType[K, V] :: HNil]
  ): TableBuilder[R,  PK, P.Out, IDX] = {
    val key = internal.keyOf(name)
    TableBuilder(ks, indexes, partitionKeys, clusterKeys :+ key, if (ascending) descendingKeys else descendingKeys + key)
  }

  /** registers given `V` as column of this table with name `name` **/
  def column[K, V](name: Witness.Aux[K])(
    implicit ev: CType[V]
  ): TableBuilder[FieldType[K,V] :: R, PK, CK, IDX] = TableBuilder(ks, indexes, partitionKeys, clusterKeys, descendingKeys)

  /** registers all columns in a given list to the table **/
  def columns[C <: HList](
    implicit CTR: CTypeRecord[C]
    , PP: Prepend[C, R]
  ): TableBuilder[PP.Out, PK, CK, IDX] = TableBuilder(ks, indexes, partitionKeys, clusterKeys, descendingKeys)

  /** creates secondary index on specified table column **/
  def indexBy[K, V](column: Witness.Aux[K], name: String, clazz: Option[String] = None, options: Map[String, String] = Map.empty)(
    implicit S: Selector.Aux[R,K,V]
  ): TableBuilder[R, PK, CK, FieldType[K, V] :: IDX] = {
    TableBuilder(ks, IndexEntry(name, internal.keyOf(column), clazz, options) +: indexes, partitionKeys, clusterKeys, descendingKeys)
  }

  /** create secondary SASI index of Prefix type **/
  def indexByPrefix[K, V](column: Witness.Aux[K], name: String, options: Map[String, String] = Map.empty)(
    implicit S: Selector.Aux[R,K,V]
  ): TableBuilder[R, PK, CK, FieldType[K, V] :: IDX] = {
    indexBy(column, name, Some(IndexEntry.SASIIndexClz), options)
  }

  /** create secondary SASI index of Contains type **/
  def indexByContains[K, V](column: Witness.Aux[K], name: String, clazz: Option[String] = None, options: Map[String, String] = Map.empty)(
    implicit S: Selector.Aux[R,K,V]
  ): TableBuilder[R, PK, CK, FieldType[K, V] :: IDX] =
    indexBy(column, name, Some(IndexEntry.SASIIndexClz), Map("mode" -> "CONTAINS") ++ options)


  /** create secondary SASI index of Sparse type **/
  def indexBySparse[K, V](column: Witness.Aux[K], name: String, clazz: Option[String] = None, options: Map[String, String] = Map.empty)(
    implicit S: Selector.Aux[R, K, V]
  ): TableBuilder[R, PK, CK, FieldType[K, V] :: IDX] =
    indexBy(column, name, Some(IndexEntry.SASIIndexClz), Map("mode" -> "SPARSE") ++ options)

  /** create SAI (Storage Attached Index) on specified column **/
  def indexBySAI[K, V](column: Witness.Aux[K], name: String, options: Map[String, String] = Map.empty)(
    implicit S: Selector.Aux[R, K, V]
  ): TableBuilder[R, PK, CK, FieldType[K, V] :: IDX] =
    indexBy(column, name, Some(IndexEntry.SAIIndexClz), options)

  /** create SAI index with specific similarity function for vector columns **/
  def indexBySAIVector[K, V](column: Witness.Aux[K], name: String, similarity: SimilarityFunction = SimilarityFunction.COSINE, options: Map[String, String] = Map.empty)(
    implicit S: Selector.Aux[R, K, V]
  ): TableBuilder[R, PK, CK, FieldType[K, V] :: IDX] =
    indexBy(column, name, Some(IndexEntry.SAIIndexClz), options + ("similarity_function" -> similarity.name))

  /** create SAI index on collection column with specified target (KEYS/VALUES/ENTRIES/FULL) **/
  def indexBySAICollection[K, V](column: Witness.Aux[K], name: String, target: CollectionIndexTarget, options: Map[String, String] = Map.empty)(
    implicit S: Selector.Aux[R, K, V]
  ): TableBuilder[R, PK, CK, FieldType[K, V] :: IDX] = {
    val entry = IndexEntry(name, internal.keyOf(column), Some(IndexEntry.SAIIndexClz), options, Some(target))
    TableBuilder(ks, entry +: indexes, partitionKeys, clusterKeys, descendingKeys)
  }

  def build(name: String, options: Map[String, String] = Map.empty)(
    implicit T: TableInstance[R, PK, CK, IDX]
  ): Table[R, PK, CK, IDX] = T.table(ks,name,options, self.indexes, self.partitionKeys, self.clusterKeys, self.descendingKeys)


}
