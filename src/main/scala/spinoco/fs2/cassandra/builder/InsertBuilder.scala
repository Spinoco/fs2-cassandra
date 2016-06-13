package spinoco.fs2.cassandra.builder

import java.nio.ByteBuffer

import com.datastax.driver.core._
import shapeless.labelled._
import shapeless.ops.record.Selector
import shapeless.tag.@@
import shapeless.{::, HList, Witness}
import spinoco.fs2.cassandra.CType.TTL
import spinoco.fs2.cassandra.internal.CTypeNonEmptyRecordInstance
import spinoco.fs2.cassandra.{Insert, Table, internal}

import scala.concurrent.duration.FiniteDuration

/**
  * Builder for Insert of the columns in table. `I` is at least sum of Partitioning and Cluster Key types
  */
case class InsertBuilder[R <: HList, I <: HList](
  table: Table[R,_ <: HList, _ <: HList]
  , ttl:Option[String]
  , timestamp: Option[String]
  , ifNotExistsFlag: Boolean
) {

  /** inserts all columns in the table **/
  def all:InsertBuilder[R, R] =
    InsertBuilder(table,None,None, ifNotExistsFlag)

  /**
    * Allows to specify TTL attribute at insert time.
    */
  def withTTL[K](wt:Witness.Aux[K]):InsertBuilder[R,  FieldType[K,FiniteDuration @@ TTL] :: I] =
    InsertBuilder(table,Some(internal.keyOf(wt)), timestamp, ifNotExistsFlag)

  /**
    * Allows to specify Timestamp attribute of the row at insert time.
    */
  def withTimestamp[K](wt:Witness.Aux[K]):InsertBuilder[R,  FieldType[K,Long] :: I] =
    InsertBuilder(table,ttl, Some(internal.keyOf(wt)), ifNotExistsFlag)

  /** flags the insert so `IF NOT EXISTS` condition is applied **/
  def ifNotExists:InsertBuilder[R,I] =
    InsertBuilder(table,ttl,timestamp, ifNotExistsFlag = true)

  /**
    * Inserts specific column to table.
    */
  def column[K,V](wt:Witness.Aux[K])(implicit ev:Selector.Aux[R,K,V])
  :InsertBuilder[R, FieldType[K,V] :: I] =
    InsertBuilder(table,ttl,timestamp, ifNotExistsFlag)

  /** creates insert statement **/
  def build(
    implicit
    CTI: CTypeNonEmptyRecordInstance[I]
    , CTR: CTypeNonEmptyRecordInstance[R]
  ):Insert[I,Option[R]] = {
    val columnNames = CTI.types.map(_._1).filterNot(k => ttl.contains(k) || timestamp.contains(k))
    val ttlStmt = ttl.map(k => s"TTL :$k").toSeq
    val timestampStmt = timestamp.map(k => s"TIMESTAMP :$k").toSeq
    val usingStmt =
      if (ttlStmt.nonEmpty || timestamp.nonEmpty) (ttlStmt ++ timestampStmt).mkString("USING "," AND ", "")
      else ""
    val ifNe = if (ifNotExistsFlag) "IF NOT EXISTS" else ""
    val cql = s"INSERT INTO ${table.keySpace}.${table.name} (${columnNames.mkString(",")}) VALUES (${columnNames.map(":" + _).mkString(",")}) $ifNe $usingStmt "

    new Insert[I,Option[R]] {
      def cqlStatement: String = cql
      def writeCql(s: I): Map[String, String] = CTI.writeCql(s)
      def writeRaw(s: I, protocolVersion: ProtocolVersion): Map[String, ByteBuffer] = CTI.writeRaw(s, protocolVersion)
      def read(r: Row, protocolVersion: ProtocolVersion): Either[Throwable, Option[R]] = {
        if (ifNotExistsFlag) {
          if (r.getBool("[applied]")) Right(None)
          else CTR.readByName(r, protocolVersion).right.map(Some(_))
        }
        else Right(None)
      }

      def fill(i: I, s: PreparedStatement, protocolVersion: ProtocolVersion): BoundStatement = {
        val bs = s.bind()
        CTI.writeByName(i,bs,protocolVersion)
        bs
      }
      def read(r: ResultSet, protocolVersion: ProtocolVersion): Either[Throwable, Option[R]] = {
        Option(r.one()) match {
          case None => Right(None)
          case Some(row) => read(row,protocolVersion)
        }
      }

      override def toString: String = s"Insert[$cql]"
    }
  }

}
