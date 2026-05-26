package spinoco.fs2.cassandra.builder

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.cql.{BoundStatement, DefaultBatchType, PreparedStatement, Row, BatchStatement => CBatchStatement}
import shapeless.{::, HList, HNil}
import spinoco.fs2.cassandra.{BatchStatement, DMLStatement}

import scala.jdk.CollectionConverters._

case class BatchBuilder[Q <: HList, R <: HList] (
  isLogged: Boolean
  , statements: Seq[String]
  , fill: (Q, Seq[PreparedStatement], ProtocolVersion) => Either[Throwable, Seq[BoundStatement]]
  , readResult: Q => (Seq[Row], ProtocolVersion) => Either[Throwable, R]
) { self =>

  /**
    * Marks batch statement to be logged
    */
  def logged: BatchBuilder[Q, R] = self.copy(isLogged = true)

  /**
    * Marks batch statement to be not logged.
    *
    * @return
    */
  def unLogged: BatchBuilder[Q, R] = self.copy(isLogged = false)

  /** appends given DML statement (INSERT, UPDATE, DELETE) to this batch **/
  def add[I, O](dml: DMLStatement[I, O]): BatchBuilder[ I :: Q, Option[O] :: R] = {
    def fillIQ(iq: I :: Q, stmts: Seq[PreparedStatement], protocolVersion: ProtocolVersion): Either[Throwable, Seq[BoundStatement]] = {
      stmts.headOption match {
        case None => Left(new Throwable(s"Failed do bind prepeared statement (no prepared statement found) ${dml.cqlStatement}"))
        case Some(ps) =>
          self.fill(iq.tail, stmts.tail, protocolVersion)
            .map { dml.fill(iq.head, ps, protocolVersion) +: _  }
      }
    }

    def readRow(iq: I :: Q)(rows: Seq[Row], protocolVersion: ProtocolVersion): Either[Throwable, Option[O] :: R] = {
      val bb = dml.readBatchResult(iq.head)
      rows.find(bb.readsFrom(_, protocolVersion)) match {
        case None => self.readResult(iq.tail)(rows, protocolVersion).map( None :: _)
        case Some(row) =>
          bb.read(row, protocolVersion).flatMap{ o => self.readResult(iq.tail)(rows, protocolVersion).map( Some(o) :: _) }
      }
    }

    BatchBuilder(
      isLogged = self.isLogged
      , statements = dml.cqlStatement +: self.statements
      , fill = fillIQ
      , readResult = readRow
    )
  }

  def build: BatchStatement[Q, R] = {
    new BatchStatement[Q, R] {
      def statements: Seq[String] =
        self.statements

      def readResult(r: Q)(rows: Seq[Row], protocolVersion: ProtocolVersion): Either[Throwable, R] = self.readResult(r)(rows, protocolVersion)

      def createStatement(statements: Seq[PreparedStatement], r: Q, protocolVersion: ProtocolVersion): Either[Throwable, CBatchStatement] =
        self.fill(r, statements, protocolVersion).map { bs =>
          val tpe = if (self.isLogged) DefaultBatchType.LOGGED else DefaultBatchType.UNLOGGED
          val batch = CBatchStatement.newInstance(tpe)
          batch.addAll(bs.asJava)
        }
    }

  }
}

object BatchBuilder {

  def apply(logged: Boolean): BatchBuilder[HNil, HNil] = {
    def fillIQ(iq: HNil, stmts: Seq[PreparedStatement], protocolVersion: ProtocolVersion): Either[Throwable, Seq[BoundStatement]] =
      Right(Nil)

    def readRow(iq: HNil)(rows: Seq[Row], protocolVersion: ProtocolVersion): Either[Throwable, HNil] =
      Right(HNil)


    BatchBuilder(
      isLogged = logged
      , statements = Nil
      , fill = fillIQ
      , readResult = readRow
    )
  }

}
