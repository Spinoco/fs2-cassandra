package spinoco.fs2.cassandra.builder

import java.nio.ByteBuffer

import com.datastax.driver.core.{BoundStatement, PreparedStatement, ProtocolVersion, Row}
import shapeless.labelled._
import shapeless.ops.hlist.{Prepend, ToTraversable}
import shapeless.ops.record.{Keys, Selector}
import shapeless.{::, HList, HNil, Witness}
import spinoco.fs2.cassandra.internal._
import spinoco.fs2.cassandra.{CQLFunction, CQLFunction0, Comparison, AbstractTable, Query, internal}
import spinoco.fs2.cassandra.util.AnnotatedException

case class QueryBuilder[R <: HList, PK <: HList, CK <: HList, IDX <: HList, Q <: HList, S <: HList, M](
  table: AbstractTable[R, PK, CK, IDX]
  , queryColumns:Seq[(String,String)]
  , whereConditions: Seq[String]
  , orderColumns:Seq[(String, Boolean)]
  , clusterColumns:Map[Comparison.Value, Seq[(String, String)]]
  , limitCount:Option[Int]
  , allowFilteringFlag:Boolean
) { self =>

  /** mark this query to contain all columns in the table as result **/
  def all:QueryBuilder[R, PK, CK, IDX, Q, R, M] =  {
    QueryBuilder(
      table = table
      , queryColumns = table.columns.map { case (n,_) => n -> n }
      , whereConditions = whereConditions
      , orderColumns = orderColumns
      , clusterColumns = clusterColumns
      , limitCount = limitCount
      , allowFilteringFlag = allowFilteringFlag
    )
  }

  /** selects given column **/
  def column[K,V](name:Witness.Aux[K])(
    implicit  ev0: Selector.Aux[R,K,V]
  ) : QueryBuilder[R, PK, CK, IDX, Q, FieldType[K,V] :: S, M] = {
    columnAs[K,V,K](name,name)
  }

  /** select given column and alias it with `as` **/
  def columnAs[K,V,K0](name:Witness.Aux[K], as:Witness.Aux[K0])(
    implicit  ev0: Selector.Aux[R,K,V]
  ): QueryBuilder[R, PK, CK, IDX, Q, FieldType[K0,V] :: S, M] = {
    QueryBuilder(
      table = table
      , queryColumns = queryColumns :+ (internal.keyOf(name) -> internal.keyOf(as))
      , whereConditions = whereConditions
      , orderColumns = orderColumns
      , clusterColumns = clusterColumns
      , limitCount = limitCount
      , allowFilteringFlag = allowFilteringFlag
    )
  }

  /** selects all columms in a given list **/
  def columns[C <: HList](
    implicit CA: ColumnsKeys[C]
    , sel: SelectAll[R, C]
    , PP: Prepend[C, S]
  ): QueryBuilder[R, PK, CK, IDX, Q, PP.Out, M] = {
    QueryBuilder(
      table = table
      , queryColumns = queryColumns ++ CA.keys.map(k => k -> k)
      , whereConditions = whereConditions
      , orderColumns = orderColumns
      , clusterColumns = clusterColumns
      , limitCount = limitCount
      , allowFilteringFlag = allowFilteringFlag
    )
  }

  /** select function `fn` applied at column `name` with alias `as` **/
  def functionAt[K,V,K0,V0](fn: CQLFunction[V,V0], name:Witness.Aux[K], as:Witness.Aux[K0])(
    implicit ev0: Selector.Aux[R,K,V]
  ): QueryBuilder[R, PK, CK, IDX, Q, FieldType[K0,V0] :: S, M] = {
    QueryBuilder(
      table = table
      , queryColumns = queryColumns :+ (fn(internal.keyOf(name)) -> internal.keyOf(as))
      , whereConditions = whereConditions
      , orderColumns = orderColumns
      , clusterColumns = clusterColumns
      , limitCount = limitCount
      , allowFilteringFlag = allowFilteringFlag
    )
  }

  /** select function from the table that does not take a parameter **/
  def function[K,V](fn: CQLFunction0[V], as:Witness.Aux[K]): QueryBuilder[R, PK, CK, IDX, Q, FieldType[K,V] :: S, M] = {
    QueryBuilder(
      table = table
      , queryColumns = queryColumns :+ (fn.apply() -> internal.keyOf(as))
      , whereConditions = whereConditions
      , orderColumns = orderColumns
      , clusterColumns = clusterColumns
      , limitCount = limitCount
      , allowFilteringFlag = allowFilteringFlag
    )
  }

  /**
    * Returns only rows, where primary key matches specified value
    */
  def partition[PKK <: HList](
    implicit
    PKK:Keys.Aux[PK, PKK]
    , ev0: ToTraversable.Aux[PKK,List,AnyRef]
  ) : QueryBuilder[R, PK, CK, IDX, PK, S, M] = {
    val pkStmts = PKK().toList.map(internal.asKeyName).map(k => s"$k = :$k")
    QueryBuilder(
      table = table
      , queryColumns = queryColumns
      , whereConditions = whereConditions ++ pkStmts
      , orderColumns = orderColumns
      , clusterColumns = clusterColumns
      , limitCount = limitCount
      , allowFilteringFlag = allowFilteringFlag
    )
  }

  /**
    * Returns only rows, where given cluster matches comparison specified
    * Note that if multiple cluster key columns constrains exists
    * this will get converted to `slice` on cluster columns.
    */
  def cluster[K,V](
    column:Witness.Aux[K]
    , op: Comparison.Value
  )(implicit
    ev0:Selector.Aux[CK, K,V]
    , P:Prepend[Q, FieldType[K,V] :: HNil]
  ): QueryBuilder[R, PK, CK, IDX, P.Out, S, M] =
    cluster(column,column,op)

  /**
    * Like `cluster` but allows to specify alias that will be used in input query
    */
  def cluster[K,K0,V](
    column:Witness.Aux[K]
    , as:Witness.Aux[K0]
    , op:Comparison.Value
  )(
    implicit
    ev0:Selector.Aux[CK, K,V]
    , P:Prepend[Q, FieldType[K0,V] :: HNil]
  ): QueryBuilder[R, PK, CK, IDX, P.Out, S, M] = {
    val k = internal.keyOf(column)
    val k0 = internal.keyOf(as)
    QueryBuilder(
      table = table
      , queryColumns = queryColumns
      , whereConditions = whereConditions
      , orderColumns = orderColumns
      , clusterColumns = clusterColumns + (op -> (clusterColumns.getOrElse(op, Nil) :+ (k -> k0)))
      , limitCount = limitCount
      , allowFilteringFlag = allowFilteringFlag
    )
  }

  /** allows to build query based on primary key (partition and all cluster keys) **/
  def primary[PKL <: HList, PKK <: HList](
   implicit
   P:Prepend.Aux[PK, CK, PKL]
   , PKK:Keys.Aux[PKL, PKK]
   , ev0: ToTraversable.Aux[PKK,List,AnyRef]
  ):QueryBuilder[R, PK, CK, IDX, PKL, S, M] = {
    val pkStmts = PKK().toList.map(internal.asKeyName).map(k => s"$k = :$k")
    QueryBuilder(
      table = table
      , queryColumns = queryColumns
      , whereConditions = pkStmts
      , orderColumns = orderColumns
      , clusterColumns = clusterColumns
      , limitCount = limitCount
      , allowFilteringFlag = allowFilteringFlag
    )
  }

  /**
    * Returns only rows for which indexed field satisfies the `op`
    */
  def byIndex[K,V](
    column:Witness.Aux[K]
    , op: Comparison.Value
  )(implicit
    ev0:Selector.Aux[IDX, K,V]
    , P:Prepend[Q, FieldType[K,V] :: HNil]
  ): QueryBuilder[R, PK, CK, IDX, P.Out, S, M] =
    byIndex(column,column,op)


  /**
    * Like `byIndex` but allows to specify alias that will be used in input query
    */
  def byIndex[K,K0,V](
     column:Witness.Aux[K]
     , as:Witness.Aux[K0]
     , op:Comparison.Value
   )(
     implicit
     ev0:Selector.Aux[IDX, K,V]
     , P:Prepend[Q, FieldType[K0,V] :: HNil]
   ): QueryBuilder[R, PK, CK, IDX, P.Out, S, M] = {
    val k = internal.keyOf(column)
    val k0 = internal.keyOf(as)
    QueryBuilder(
      table = table
      , queryColumns = queryColumns
      , whereConditions = whereConditions :+ s"$k $op :$k0"
      , orderColumns = orderColumns
      , clusterColumns = clusterColumns
      , limitCount = limitCount
      , allowFilteringFlag = allowFilteringFlag
    )
  }


  /**
    * Allows to pass limit of records that has to be returned
    */
  def limit(max:Int):QueryBuilder[R,  PK, CK, IDX, Q, S, M] = {
    copy(limitCount = Some(max))
  }

  /** sets flag to indicate the query may support ALLOW FILTERING **/
  def allowFiltering:QueryBuilder[R,  PK, CK, IDX, Q, S, M] =
    copy(allowFilteringFlag = true)


  /** allows to order the results baseon on given cluster column **/
  def orderBy[K, V](name:Witness.Aux[K], ascending:Boolean)(
    implicit ev0:Selector.Aux[CK, K,V]
  ):QueryBuilder[R, PK, CK, IDX,  Q, S, M] =
    copy( orderColumns = orderColumns :+ (internal.keyOf(name) -> ascending))

  /** creates query, that may be used to perform CQL commands on connection **/
  def build(
   implicit CTQ: CTypeRecordInstance[Q]
   , CTS: CTypeNonEmptyRecordInstance[S]
  ):Query[Q,S] = {
    val orderStmt = {
      val ocs =
      orderColumns.map {
        case (k, asc) => s"$k ${if(asc) "ASC" else "DESC"}"
      }.mkString("," )
      if (ocs.nonEmpty) s"ORDER BY $ocs" else ""
    }


    val allowFilteringStmt =
      if (allowFilteringFlag) "ALLOW FILTERING" else ""

    val limitStmt = limitCount.map(c => s"LIMIT $c").mkString

    val clusterKeyStatements = {
      clusterColumns.map { case (op, columns) =>
        if (columns.size > 1) {
          val cs = columns.map(_._1).mkString("(",",",")")
          val vs = columns.map(_._2).map(":"+_).mkString("(",",",")")
          s"$cs $op $vs"
        } else {
          columns.map { case(c,n) => s"$c $op :$n"}.mkString
        }

      }
    }

    val whereStmt :String = {
      val cond = (
        whereConditions ++
          clusterKeyStatements
        )
      .filter(_.nonEmpty)
      .mkString(" AND ")

      if (cond.nonEmpty) s"WHERE $cond" else ""
    }

    val selectColumns:String = {
      queryColumns.map { case (sel,as) =>
        if (sel == as) sel
        else s"$sel AS $as"
      }.mkString(",")
    }

    val cql = Seq(
      s"SELECT $selectColumns FROM ${table.keySpaceName}.${table.name}"
      , whereStmt
      , orderStmt
      , limitStmt
      , allowFilteringStmt
    )
    .filter(_.nonEmpty)
    .mkString(" ")


    new Query[Q, S] {
      def cqlStatement: String = cql
      def cqlFor(q: Q): String = spinoco.fs2.cassandra.util.replaceInCql(cql,CTQ.writeCql(q))
      def writeRaw(q: Q, protocolVersion: ProtocolVersion): Map[String, ByteBuffer] = CTQ.writeRaw(q,protocolVersion)
      def read(r: Row, protocolVersion: ProtocolVersion): Either[Throwable, S] = CTS.readByName(r, protocolVersion).left.map(AnnotatedException.withStmt(_, cql))
      def fill(q: Q, s: PreparedStatement, protocolVersion: ProtocolVersion): BoundStatement = {
        val bs = s.bind()
        CTQ.writeByName(q,bs,protocolVersion)
        bs
      }

      override def toString: String = s"Query[$cql]"
    }

  }
}

object QueryBuilder{

  implicit class TableQueryBuilderSyntax[R <: HList, PK <: HList, CK <: HList, IDX <: HList, Q <: HList, S <: HList](
    val self: QueryBuilder[R, PK, CK, IDX, Q, S, Materializable]
  ) extends AnyVal {

    /** starts creating a materialized view out of this query **/
    def materialize: MaterializedViewBuilder[R, PK, CK, S, HNil, HNil] =
      MaterializedViewBuilder(self, Nil, Nil)
  }

}
