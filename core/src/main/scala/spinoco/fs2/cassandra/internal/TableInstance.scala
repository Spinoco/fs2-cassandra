package spinoco.fs2.cassandra.internal

import com.datastax.driver.core.DataType
import shapeless.ops.hlist.Prepend
import shapeless.ops.record.Keys
import shapeless.{HList, HNil}
import spinoco.fs2.cassandra._
import spinoco.fs2.cassandra.builder._

/**
  * Created by pach on 03/06/16.
  */
trait TableInstance[R <: HList, PK <: HList, CK <: HList, IDX <: HList] {

  def table(
     ks:KeySpace
     , name:String
     , options:Map[String, String]
     , indexes:Seq[IndexEntry]
     , partitionKeys:Seq[String]
     , clusterKeys:Seq[String]
  ):Table[R,PK, CK, IDX]
}


object TableInstance {


  implicit def forProduct[R <: HList, PK <: HList,  CK <: HList,  IDX <: HList](
    implicit
     RKS: Keys[R]
    , CTR: CTypeNonEmptyRecordInstance[R]
  ):TableInstance[R, PK, CK, IDX] = {
    new TableInstance[R,PK,CK, IDX] {

      def table(
        ks:KeySpace
        , tn:String
        , opt:Map[String, String]
        , idxs:Seq[IndexEntry]
        , pks:Seq[String]
        , cks:Seq[String]
      ):Table[R,PK, CK, IDX] =
        new Table[R, PK, CK, IDX] { self =>
          val fields = CTR.types.map { case (k,tpe) =>
            s"$k ${tpe.toString()}"
          }.mkString(",")
          val pkDef = {
            if (cks.isEmpty) s"(${pks.mkString(",")})"
            else s"(${pks.mkString(",")}),${cks.mkString(",")}"
          }

          val cql:String =
            s"""CREATE TABLE ${ks.name}.$tn ($fields, PRIMARY KEY ($pkDef))"""

          val indexCql =
            idxs.map(_.cqlStatement(ks.name,tn))

          def cqlStatement:Seq[String] = cql +: indexCql
          def keySpace:KeySpace = ks
          def name:String = tn
          def options:Map[String,String] = opt
          def columns: Seq[(String, DataType)] = CTR.types
          def partitionKey: Seq[String] = pks
          def clusterKey: Seq[String] = cks

          def query: QueryBuilder[R, PK, CK, IDX, HNil, HNil] =
            QueryBuilder(self, Nil, Nil, Nil, Map.empty, None,None, allowFilteringFlag = false)

          def insert(implicit p:Prepend[PK,CK]):InsertBuilder[R,PK,CK,p.Out] =
            InsertBuilder(self, None, None, ifNotExistsFlag = false)

          def delete[Q, R0]: DeleteBuilder[R, PK, CK, PK, HNil] =
            DeleteBuilder(self,Nil,Nil,None,ifExistsCondition = false)

          def update(implicit p:Prepend[PK,CK]):UpdateBuilder[R, PK, CK, p.Out,HNil] =
            UpdateBuilder(self,Nil,Set.empty,Nil, None, None, Nil, ifExistsCondition = false)

          def indexes:Seq[IndexEntry] = idxs

          override def toString: String = s"Table[$cql]"
        }
    }
  }





}

