package spinoco.fs2.cassandra.builder


import java.nio.ByteBuffer

import com.datastax.driver.core._
import shapeless.labelled._
import shapeless.{::, HList, HNil, Witness}
import shapeless.ops.record.Selector
import shapeless.tag._
import spinoco.fs2.cassandra.CType.{Counter, TTL}
import spinoco.fs2.cassandra.builder.UpdateBuilder.IfExistsField
import spinoco.fs2.cassandra.internal._
import spinoco.fs2.cassandra.{BatchResultReader, Comparison, Table, Update, internal}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import spinoco.fs2.cassandra.util.AnnotatedException

/**
  * Builder of update statement
  *
  * @param table                  Table this operates on
  * @param collectionUpdates       Updates to collection fields (set, list, map)
  * @param ifConditions           If conditions
  * @param ifExistsCondition      flag indicating if this has to be applied only if row exists.
  */
case class UpdateBuilder[R <: HList, PK <: HList, CK <: HList, Q <: HList, RIF <: HList] (
 table: Table[R,PK, CK, _ <: HList]
 , collectionUpdates:Seq[String]
 , collectionKeys:Set[String]
 , ifConditions: Seq[(String,String, Comparison.Value)]
 , timestamp:Option[String]
 , ttl: Option[String]
 , counterColumns: Seq[(String,Boolean)]
 , ifExistsCondition: Boolean
 ) {

  /**
    * Causes to update all elements of single row specified by primary key.
    * `Q` is set to primary key and `S` type is set to type of all columns except columns specified in primary key.
    *
    * @return
    */
  def all:UpdateBuilder[R,PK,CK, R, HNil] =
    UpdateBuilder(table,Nil,Set.empty,Nil,None, None, Nil, false)

  /**
    * Sets only this given column. Appends to columns already specified in `set`
    */
  def set[K,V](wt:Witness.Aux[K])(
    implicit
    ev:Selector.Aux[R,K,V]
  )
  :UpdateBuilder[R,PK,CK,FieldType[K,V] :: Q, RIF] =
    UpdateBuilder(table,collectionUpdates,collectionKeys,ifConditions,timestamp, ttl, counterColumns, ifExistsCondition)

  /** appends element to column of list type (i.e. List, Seq, Vector) **/
  def append[C <: Seq[_],K](wt:Witness.Aux[K])(
    implicit
    ev0:Selector.Aux[R,K,C]
  ):UpdateBuilder[R,PK,CK,FieldType[K,C] :: Q, RIF] =
    append(wt,wt)

  /** appends element to column of list type (i.e. List, Seq, Vector), allowing input by tagged as `as` **/
  def append[C <: Seq[_],K, KK](wt:Witness.Aux[K], as:Witness.Aux[KK])(
    implicit
    ev0:Selector.Aux[R,K,C]
  ):UpdateBuilder[R,PK,CK,FieldType[KK,C] :: Q, RIF] = {
    val k = internal.keyOf(wt)
    val kk = internal.keyOf(as)
    collectionOp(kk,s"$k = $k + :$kk")
  }

  /** prepends element to column of list type (i.e. List, Seq, Vector) **/
  def prepend[C <: Seq[_],K](wt:Witness.Aux[K])(
    implicit
    ev0:Selector.Aux[R,K,C]
  ):UpdateBuilder[R,PK,CK,FieldType[K,C] :: Q, RIF] =
    prepend(wt,wt)

  /** prepends element to column of list type (i.e. List, Seq, Vector), allowing input by tagged as `as` **/
  def prepend[C <: Seq[_],K, KK](wt:Witness.Aux[K], as:Witness.Aux[KK])(
    implicit
    ev0:Selector.Aux[R,K,C]
  ):UpdateBuilder[R,PK,CK,FieldType[K,C] :: Q, RIF] = {
    val k = internal.keyOf(wt)
    val kk = internal.keyOf(as)
    collectionOp(kk,s"$k = :$kk + $k")
  }

  /** sets value at given index in list **/
  def addAt[C[_],K,V](name:Witness.Aux[K], index:Int)(
    implicit
    ev0:Selector.Aux[R,K,C[V]]
    ,ev1:ListColumnInstance[C,V]
  ):UpdateBuilder[R,PK,CK,FieldType[K,V] :: Q, RIF] = {
    val k = internal.keyOf(name)
    val k0 = internal.keyOf(index)
    UpdateBuilder(
      table
      , collectionUpdates :+ s"$k[$index] = :$k"
      , collectionKeys ++ Set(k,k0)
      , ifConditions
      , timestamp
      , ttl
      , counterColumns
      , ifExistsCondition
    )
  }

  /** append given elements to map column**/
  def addToMap[M <: Map[_,_],K,V, VK](name:Witness.Aux[K])(
    implicit
     ev:Selector.Aux[R,K,M]
  ):UpdateBuilder[R,PK,CK,FieldType[K,M] :: Q, RIF] =
    addToMap(name,name)


  /** append given elements to map column**/
  def addToMap[M <: Map[_,_],K,KK](name:Witness.Aux[K], as:Witness.Aux[KK])(
    implicit
    ev0:Selector.Aux[R,K,M]
  ):UpdateBuilder[R,PK,CK,FieldType[KK,M]  :: Q, RIF] = {
    val k = internal.keyOf(name)
    val kk = internal.keyOf(as)
    collectionOp(kk,s"$k = $k + :$kk")
  }

  /** adds given value to column of set type **/
  def add[C <: Set[_],K,V](wt:Witness.Aux[K])(
    implicit
    ev0:Selector.Aux[R,K,C]
  ):UpdateBuilder[R,PK,CK,FieldType[K,C] :: Q, RIF] =
    add(wt,wt)

  /** adds given value to column of set type, allowing input by tagged as `as` **/
  def add[C <: Set[_],K,KK,V](wt:Witness.Aux[K],as:Witness.Aux[KK])(
    implicit
    ev0:Selector.Aux[R,K,C]
  ):UpdateBuilder[R,PK,CK,FieldType[KK,C] :: Q, RIF] = {
    val k = internal.keyOf(wt)
    val kk = internal.keyOf(as)
    collectionOp(k,s"$k = $k + :$kk")
  }

  /** removes given value from the set of list collection column **/
  def remove[C <: Iterable[_],K](wt:Witness.Aux[K])(
    implicit
    ev1:Selector.Aux[R,K,C]
  ):UpdateBuilder[R,PK,CK,FieldType[K,C] :: Q, RIF] =
    remove(wt,wt)

  /** removes given value from the set of list collection column, allowing input by tagged as `as` **/
  def remove[C <: Iterable[_],K, KK](wt:Witness.Aux[K], as:Witness.Aux[KK])(
    implicit
    ev1:Selector.Aux[R,K,C]
  ):UpdateBuilder[R,PK,CK,FieldType[KK,C] :: Q, RIF] = {
    val k = internal.keyOf(wt)
    val kk = internal.keyOf(as)
    collectionOp(kk,s"$k = $k - :$kk")
  }

  /** removes given keys from the map column. multiple (possibly zero) keys may be specified **/
  def removeFromMap[M <: Map[_,_],K,V, VK](name:Witness.Aux[K])(
    implicit
    ev0:Selector.Aux[R,K,M]
    , ev1: MapColumnInstance.Aux[M,VK, V]
  ):UpdateBuilder[R,PK,CK,FieldType[K,Set[VK]] :: Q, RIF] =
    removeFromMap[M,K,K,V,VK](name,name)


  /** removes given keys from the map column. multiple (possibly zero) keys may be specified **/
  def removeFromMap[M <: Map[_,_],K,KK, V, VK](name:Witness.Aux[K], as:Witness.Aux[KK])(
    implicit
    ev0:Selector.Aux[R,K,M]
    , ev1: MapColumnInstance.Aux[M,VK, V]
  ):UpdateBuilder[R,PK,CK,FieldType[KK,Set[VK]] :: Q, RIF] = {
    val k = internal.keyOf(name)
    val kk = internal.keyOf(as)
    collectionOp(kk,s"$k = $k - :$kk")
  }


  private def collectionOp[Q0 <: HList](k:String,op:String):UpdateBuilder[R,PK,CK,Q0, RIF] = {
    UpdateBuilder(
      table
      , collectionUpdates :+ op
      , collectionKeys + k
      , ifConditions
      , timestamp
      , ttl
      , counterColumns
      , ifExistsCondition
    )
  }

  /**
    * Allows to specify TTL attribute at update time.
    */
  def withTTL[K](wt:Witness.Aux[K]):UpdateBuilder[R,PK,CK,FieldType[K,FiniteDuration @@ TTL] :: Q,RIF] =
    UpdateBuilder(
      table
      , collectionUpdates
      , collectionKeys
      , ifConditions
      , timestamp
      , Some(internal.keyOf(wt))
      , counterColumns
      , ifExistsCondition
    )

  /**
    * Allows to specify Timestamp attribute of the row at update time.
    */
  def withTimeStamp[K](wt:Witness.Aux[K]):UpdateBuilder[R,PK,CK,FieldType[K,Long] :: Q,RIF] =
    UpdateBuilder(
      table
      , collectionUpdates
      , collectionKeys
      , ifConditions
      , Some(internal.keyOf(wt))
      , ttl
      , counterColumns
      , ifExistsCondition
    )

  /** increments supplied counter column **/
  def increment[K](wt:Witness.Aux[K])
   (implicit ev0:Selector.Aux[R,K,Long @@ Counter])
  :UpdateBuilder[R,PK,CK,FieldType[K,Long] :: Q,RIF] =
    UpdateBuilder(
      table
      , collectionUpdates
      , collectionKeys
      , ifConditions
      , timestamp
      , ttl
      , counterColumns :+ (internal.keyOf(wt) -> true)
      , ifExistsCondition
    )

  /** decrements supplied counter column **/
  def decrement[K](wt:Witness.Aux[K])
  (implicit ev0:Selector.Aux[R,K,Long @@ Counter])
  :UpdateBuilder[R,PK,CK,FieldType[K,Long] :: Q,RIF] =
    UpdateBuilder(
      table
      , collectionUpdates
      , collectionKeys
      , ifConditions
      , timestamp
      , ttl
      , counterColumns :+ (internal.keyOf(wt) -> false)
      , ifExistsCondition
    )

  /**
    * Causes to update the column(s) only if they already exists.
    * Returns field with key [exists] that either is set to false, if the field existed or false otherwise.
    */
  def onlyIfExists:UpdateBuilder[R,PK,CK,Q, IfExistsField :: RIF] =
    UpdateBuilder(table,collectionUpdates,collectionKeys,ifConditions, timestamp, ttl, counterColumns, true)


  /**
    * Causes to specify `IF` condition to guard the update operation.
    * Returns optional field-type, that is set to None in case the operation was successful
    * or Some(current_value) in case the condition failed.
    */
  def onlyIf[K, V](name:Witness.Aux[K], op:Comparison.Value)(
    implicit ev: Selector.Aux[R,K,V]
  ):UpdateBuilder[R,PK,CK,FieldType[K,V] :: Q, FieldType[K,Option[V]] :: RIF] =
    onlyIf(name,name, op)


  /**
    * Like onlyIf, but allows to specify alias apart form the name of the column.
    * Result contains field with that configured alias.
    */
  def onlyIf[K, V, K0](name:Witness.Aux[K], as:Witness.Aux[K0], op:Comparison.Value)(
    implicit ev: Selector.Aux[R,K,V]
  ):UpdateBuilder[R,PK,CK,FieldType[K0,V] :: Q, FieldType[K,Option[V]] :: RIF] = {
    UpdateBuilder(
      table
      , collectionUpdates
      , collectionKeys
      , ifConditions :+ ((internal.keyOf(name), internal.keyOf(as), op))
      , timestamp
      , ttl
      , counterColumns
      , ifExistsCondition
    )
  }

  /**
    * Create Update that may be used to perform UPDATE DML against the Table
    */
  def build(
    implicit
    CTQ: CTypeNonEmptyRecordInstance[Q]
    , CTR: CTypeRecordInstance[RIF]
  ): Update[Q, RIF] = {
    val ifExistsStmt = if (ifExistsCondition) " IF EXISTS" else ""
    val ifStmts = ifConditions.map { case (c,as, op) =>  s"$c $op :$as" }.mkString(" AND ")
    val ifStmt = if (ifStmts.nonEmpty) s" IF $ifStmts" else ""
    val whereStmt = (table.partitionKey ++ table.clusterKey).map { k => s"$k = :$k" }.mkString(" AND ")
    val fieldSetStmt =
      CTQ.types
        .filterNot{ case (k,_) =>
          collectionKeys.contains(k) ||
            table.partitionKey.contains(k) ||
            table.clusterKey.contains(k) ||
            ttl.contains(k) ||
            timestamp.contains(k) ||
            counterColumns.exists { case (n, _) => n == k } ||
            ifConditions.exists{ case (_,ik,_) =>  ik == k }
        }
        .map{ case (k,_) => s"$k = :$k"}
    val collectionSetStmt = collectionUpdates
    val counterStmt = counterColumns.map { case (name, increment) =>
      val op = if (increment) "+" else "-"
       s"$name = $name $op :$name"
    }
    val setStmt = (fieldSetStmt ++ collectionSetStmt ++ counterStmt).mkString(",")
    val usingStmt =
      if (ttl.nonEmpty || timestamp.nonEmpty) (ttl.toSeq.map { c => s" TTL :$c" } ++ timestamp.toSeq.map {c => s"TIMESTAMP :$c"}).mkString(" USING "," AND ", " ")
      else ""

    val cql = s"UPDATE ${table.keySpaceName}.${table.name}$usingStmt SET $setStmt WHERE $whereStmt$ifStmt$ifExistsStmt"

    new Update[Q,RIF] {
      def cqlStatement: String = cql
      def cqlFor(q: Q): String = spinoco.fs2.cassandra.util.replaceInCql(cql,CTQ.writeCql(q))
      def writeRaw(q: Q, protocolVersion: ProtocolVersion): Map[String, ByteBuffer] = CTQ.writeRaw(q, protocolVersion)
      def read(r: Row, protocolVersion: ProtocolVersion): Either[Throwable, RIF] = {
        CTR.readByName(r,protocolVersion).left.map(AnnotatedException.withStmt(_, cql))
      }
      def fill(i: Q, s: PreparedStatement, protocolVersion: ProtocolVersion): BoundStatement = {
        val bs = s.bind()
        CTQ.writeByName(i,bs,protocolVersion)
        bs
      }
      def read(r: ResultSet, protocolVersion: ProtocolVersion): Either[Throwable, RIF] = {
        (Option(r.one()) match {
          case None =>
            if (ifExistsCondition || ifConditions.nonEmpty) Left(new Throwable("Expected update result but got nothing"))
            else Right(HNil.asInstanceOf[RIF]) // safe hence result must be always empty HList (HNil) in this case
          case Some(row) =>
            val columns = r.getColumnDefinitions.asList().asScala.map(_.getName).toSet
            CTR.readByNameIfExists(columns,row,protocolVersion)
        }).left.map(AnnotatedException.withStmt(_, cql))
      }


      def readBatchResult(i: Q): BatchResultReader[RIF] = {
        new BatchResultReader[RIF] {
          def readsFrom(row: Row, protocolVersion: ProtocolVersion): Boolean =
            CTQ.readByName(row,protocolVersion).fold(_ => false, _ == i)

          def read(row: Row, protocolVersion: ProtocolVersion): Either[Throwable, RIF] =
            CTR.readByName(row,protocolVersion)

        }
      }

      override def toString: String = s"Update[$cql]"
    }
  }

}

object UpdateBuilder {

  type IfExistsField = Witness.`"[applied]"`.->>[Boolean]


}
