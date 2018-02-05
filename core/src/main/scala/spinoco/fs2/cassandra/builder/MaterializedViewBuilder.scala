package spinoco.fs2.cassandra.builder

import com.datastax.driver.core.DataType
import shapeless.labelled.FieldType
import shapeless.ops.hlist.{Prepend}
import shapeless.ops.record.{Selector}
import shapeless.{::, HList, Witness}
import spinoco.fs2.cassandra.internal.{CTypeNonEmptyRecordInstance, SelectAll}
import spinoco.fs2.cassandra.{KeySpace, MaterializedView, AbstractTable, internal}

/**
  * Helper to build type safe definition of a materialized view
  */
case class MaterializedViewBuilder[R <: HList, QPK <: HList, QCK <: HList , S <: HList, PK <: HList, CK <: HList](
  query: QueryBuilder[R, QPK, QCK, _ <: HList, _ <: HList , S, _]
  , partitionKeys: Seq[String]
  , clusterKeys: Seq[String]
){ self =>

  /** Sets a partition key for the view **/
  def partition[K, V, QPKK <: HList](name: Witness.Aux[K])(
    implicit ev0: Selector.Aux[R, K, V]
  ): MaterializedViewBuilder[R, QPK, QCK, S, FieldType[K, V] :: PK, CK] =
    MaterializedViewBuilder(query, partitionKeys :+ internal.keyOf(name), clusterKeys)

  /** Sets a cluster key for the view **/
  def cluster[K, V](name: Witness.Aux[K])(
    implicit ev0: Selector.Aux[R, K, V]
  ): MaterializedViewBuilder[R, QPK, QCK, S, PK, FieldType[K, V] :: CK] =
    MaterializedViewBuilder(query, partitionKeys, clusterKeys :+ internal.keyOf(name))

  /** Builds this view **/
  def build[PKN <: HList, QPKK <: HList, PKK <: HList, MR <: HList](
    viewName: String
    , viewOptions: Map[String, String] = Map()
  )(
    implicit p0: Prepend.Aux[PK, CK, PKK]
    , p1: Prepend.Aux[QPK, QCK, QPKK]
    , sel: SelectAll[PKK, QPKK]
    , p2: Prepend.Aux[S, PKK, MR]
    , CTR: CTypeNonEmptyRecordInstance[MR]
  ): MaterializedView[S, PK, CK] = {

    val selectColumns: String =
      query.queryColumns.map{_._1}.mkString(",")

    val select = s"SELECT $selectColumns FROM ${query.table.fullName}"
    val whereStmt: String = {
      ((partitionKeys ++ clusterKeys).toList.map(internal.asKeyName).map(k => s"$k IS NOT NULL")
        ++ query.whereConditions)
      .filter(_.nonEmpty)
      .mkString("WHERE "," AND ", "")
    }

    val pkDef = {
      if (clusterKeys.isEmpty) s"(${partitionKeys.mkString(",")})"
      else s"(${partitionKeys.mkString(",")}),${clusterKeys.mkString(",")}"
    }

    val opts = {
      if (viewOptions.isEmpty) ""
      else {
        viewOptions.toSeq
        .map{case (key, value) => s"$key = $value"}
        .mkString(" WITH ", " AND ", "")
      }
    }

    val cql =
      s"CREATE MATERIALIZED VIEW ${query.table.keySpaceName}.$viewName AS $select $whereStmt PRIMARY KEY ($pkDef)$opts"

    new MaterializedView[S, PK, CK] { self =>
      def columns: Seq[(String, DataType)] = CTR.types
      def name: String = viewName
      def keySpace: KeySpace = query.table.keySpace
      def cqlStatement: Seq[String] = Seq(cql)
      def options: Map[String, String] = viewOptions
      def partitionKey: Seq[String] = partitionKeys
      def clusterKey: Seq[String] = clusterKeys
      def table: AbstractTable[_,_,_,_] = query.table
    }
  }
}
