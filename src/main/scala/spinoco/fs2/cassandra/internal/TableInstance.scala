package spinoco.fs2.cassandra.internal

import com.datastax.driver.core.DataType
import shapeless.ops.hlist.{Prepend, ToTraversable}
import shapeless.ops.record.Keys
import shapeless.{HList, HNil}
import spinoco.fs2.cassandra._
import spinoco.fs2.cassandra.builder.{DeleteBuilder, InsertBuilder, QueryBuilder, UpdateBuilder}

/**
  * Created by pach on 03/06/16.
  */
trait TableInstance[R <: HList, PK <: HList, CK <: HList] {

  def table(ks:KeySpace,name:String, options:Map[String, String]):Table[R,PK, CK]
}


object TableInstance {


  implicit def forProduct[R <: HList, PK <: HList, PKK <: HList, CK <: HList, CKK <: HList](
    implicit
     RKS: Keys[R]
    , PKS: Keys.Aux[PK, PKK]
    , CKS: Keys.Aux[CK, CKK]
    , CTR: CTypeNonEmptyRecordInstance[R]
    , ev0: ToTraversable.Aux[PKK,List,AnyRef]
    , ev1: ToTraversable.Aux[CKK,List,AnyRef]
  ):TableInstance[R, PK, CK] = {
    new TableInstance[R,PK,CK] {

      val pks:Seq[String] = PKS().toList.map(asKeyName)
      val cks:Seq[String] = CKS().toList.map(asKeyName)


      def table(ks:KeySpace, tn:String, opt:Map[String, String] = Map.empty):Table[R,PK, CK] =
        new Table[R, PK, CK] { self =>
          val fields = CTR.types.map { case (k,tpe) =>
            s"$k ${tpe.asFunctionParameterString()}"
          }.mkString(",")
          val pkDef = {
            if (cks.isEmpty) s"(${pks.mkString(",")})"
            else s"(${pks.mkString(",")}),${cks.mkString(",")}"
          }

          val cql:String =
            s"""CREATE TABLE ${ks.name}.$tn ($fields, PRIMARY KEY ($pkDef))"""

          def cqlStatement:String = cql
          def keySpace:KeySpace = ks
          def name:String = tn
          def options:Map[String,String] = opt
          def columns: Seq[(String, DataType)] = CTR.types
          def partitionKey: Seq[String] = pks
          def clusterKey: Seq[String] = cks

          def query: QueryBuilder[R, PK, CK, HNil, HNil] =
            QueryBuilder(self, Nil, Nil, Nil, Map.empty, None,None, allowFilteringFlag = false)

          def insert(implicit p:Prepend[PK,CK]):InsertBuilder[R,PK,CK,p.Out] =
            InsertBuilder(self, None, None, ifNotExistsFlag = false)

          def delete[Q, R0]: DeleteBuilder[R, PK, CK, PK, HNil] =
            DeleteBuilder(self,Nil,Nil,None,ifExistsCondition = false)

          def update(implicit p:Prepend[PK,CK]):UpdateBuilder[R, PK, CK, p.Out,HNil] =
            UpdateBuilder(self,Nil,Set.empty,Nil, None, None, Nil, ifExistsCondition = false)


          override def toString: String = s"Table[$cql]"
        }
    }
  }





}

