package spinoco.fs2.cassandra.builder

import java.nio.ByteBuffer

import com.datastax.driver.core._
import shapeless.labelled._
import shapeless.ops.hlist.Prepend
import shapeless.ops.record.Selector
import shapeless.{::, HList, HNil, Witness}
import spinoco.fs2.cassandra.builder.UpdateBuilder.IfExistsField
import spinoco.fs2.cassandra.internal.{CTypeNonEmptyRecordInstance, CTypeRecordInstance}
import spinoco.fs2.cassandra.{BatchResultReader, Comparison, Delete, Table, internal}

import collection.JavaConverters._

case class DeleteBuilder[R <: HList, PK <: HList, CK <: HList, Q <: HList, RIF <: HList](
  table: Table[R,PK, CK]
  , ifConditions: Seq[(String,String, Comparison.Value)]
  , columns: Seq[String]
  , timestamp: Option[String]
  , ifExistsCondition: Boolean
) {

  /** deletes all columns in the specified row by PK **/
  def row:DeleteBuilder[R,PK,CK, Q, HNil] =
    DeleteBuilder(table,Nil,Nil,timestamp,ifExistsCondition=false)

  /** deletes given column from the table. Note that column value must be optional **/
  def column[K,V <: Option[_]](name:Witness.Aux[K])(
    implicit ev: Selector.Aux[R,K,V]
  ):DeleteBuilder[R,PK,CK, Q, RIF] =
    DeleteBuilder(table,Nil,columns :+ internal.keyOf(name),timestamp,ifExistsCondition=false)


  /** adds supplied column to where query **/
  def cluster[K,V](name:Witness.Aux[K])(
    implicit
    ev: Selector.Aux[CK,K,V]
    , P:Prepend[Q, FieldType[K,V] :: HNil]
  ):DeleteBuilder[R,PK,CK, P.Out, RIF] = {
    DeleteBuilder(table,ifConditions, columns, timestamp,ifExistsCondition)
  }

  /** deletes from the cluster column specified by primary key **/
  def primary( implicit P:Prepend[PK,CK]):DeleteBuilder[R,PK,CK, P.Out, RIF] =
    DeleteBuilder(table,ifConditions, columns, timestamp,ifExistsCondition)

  /** deltes only if deleted colum(s) have specified timestamp set **/
  def withTimeStamp[K](name:Witness.Aux[K]): DeleteBuilder[R,PK,CK, FieldType[K,Long] :: Q, RIF] =
    DeleteBuilder(table,ifConditions,columns,Some(internal.keyOf(name)), ifExistsCondition)


  /**
    * Deletes column(s), but only if given columns  exists
    */
  def onlyIfExists[K, V, K0]:DeleteBuilder[R,PK,CK,Q, IfExistsField :: RIF] =
    DeleteBuilder(table,ifConditions, columns, timestamp, ifExistsCondition = true)

  /**
    * Causes to specify `IF` condition to guard the delete operation.
    * Returns optional field-type, that is set to None in case the operation was successful
    * or Some(current_value) in case the condition failed.
    */
  def onlyIf[K, V](name:Witness.Aux[K], op:Comparison.Value)(
    implicit ev: Selector.Aux[R,K,V]
  ):DeleteBuilder[R,PK,CK,FieldType[K,V] :: Q, FieldType[K,Option[V]] :: RIF] =
    onlyIf(name,name, op)


  /**
    * Like onlyIf, but allows to specify alias apart form the name of the column.
    * Result contains field with that configured alias.
    */
  def onlyIf[K, V, K0](name:Witness.Aux[K], as:Witness.Aux[K0], op:Comparison.Value)(
    implicit ev: Selector.Aux[R,K,V]
  ):DeleteBuilder[R,PK,CK,FieldType[K0,V] :: Q, FieldType[K,Option[V]] :: RIF] = {
    DeleteBuilder(
      table
      , ifConditions :+ ((internal.keyOf(name), internal.keyOf(as), op))
      , columns
      , timestamp
      , ifExistsCondition
    )
  }

  /** create the DELETE statement **/
  def build(
    implicit
    CTQ: CTypeNonEmptyRecordInstance[Q]
    , CTR: CTypeRecordInstance[RIF]
  ):Delete[Q,RIF] = {
    val columnsStmts = if (columns.nonEmpty)columns.mkString(" ",",","") else ""
    val whereClause =
      CTQ.types.filterNot{ case (name,_) =>
        ifConditions.exists{ case (_,a, _) => a == name}
      }
      .map { case (k,_) => s"$k = :$k"}.mkString(" AND ")
    val ifExistsStmt = if (ifExistsCondition) " IF EXISTS" else ""
    val ifStmts = ifConditions.map { case (c,as, op) =>  s"$c $op :$as" }.mkString(" AND ")
    val ifStmt = if (ifStmts.nonEmpty) s"IF $ifStmts" else ""
    val timestampStmt = if (timestamp.nonEmpty) timestamp.map(k => s"USING TIMESTAMP :$k").mkString else ""

    val cql = s"DELETE$columnsStmts FROM ${table.keySpace}.${table.name} $timestampStmt WHERE $whereClause $ifStmt$ifExistsStmt"

    new Delete[Q,RIF] {
      def cqlStatement: String = cql
      def cqlFor(q: Q): String = spinoco.fs2.cassandra.util.replaceInCql(cql,CTQ.writeCql(q))
      def writeRaw(q: Q, protocolVersion: ProtocolVersion): Map[String, ByteBuffer] = CTQ.writeRaw(q,protocolVersion)
      def read(r: Row, protocolVersion: ProtocolVersion): Either[Throwable, RIF] = CTR.readByName(r,protocolVersion)



      def fill(i: Q, s: PreparedStatement, protocolVersion: ProtocolVersion): BoundStatement = {
        val bs = s.bind()
        CTQ.writeByName(i,bs,protocolVersion)
        bs
      }

      def read(r: ResultSet, protocolVersion: ProtocolVersion): Either[Throwable, RIF] = {
        Option(r.one()) match {
          case None =>
            if (!ifExistsCondition && ifConditions.isEmpty) Right(HNil.asInstanceOf[RIF]) // guaranteed to be safe always Hnil result if no ifExists or conditions
            else Left(new Throwable("Expected result row but none returned"))
          case Some(row) =>
            val keys = r.getColumnDefinitions.asScala.map(_.getName.toLowerCase).toSet
            CTR.readByNameIfExists(keys,row,protocolVersion)
        }
      }


      def readBatchResult(i: Q): BatchResultReader[RIF] = {
        new BatchResultReader[RIF] {
          def readsFrom(row: Row, protocolVersion: ProtocolVersion): Boolean =
            CTQ.readByName(row,protocolVersion).fold(_ => false, _ == i)

          def read(row: Row, protocolVersion: ProtocolVersion): Either[Throwable, RIF] =
            CTR.readByName(row,protocolVersion)

        }
      }

      override def toString: String = s"Delete[$cql]"
    }

  }


}

