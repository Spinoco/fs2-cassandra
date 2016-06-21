package spinoco.fs2.cassandra

import com.datastax.driver.core.{PreparedStatement, ProtocolVersion, ResultSet, Row, BatchStatement => CBatchStatement}
import shapeless.ops.hlist.Tupler
import shapeless.ops.product.ToHList
import shapeless.{Generic, HList, HNil}
import spinoco.fs2.cassandra.builder.BatchBuilder

/**
  * Created by pach on 14/06/16.
  */
object batch {

  /** creates empty, logged batch statement **/
  def logged:BatchBuilder[HNil,HNil] =
    BatchBuilder(logged = true)

  /** creates empty, unLogged batch statement **/
  def unLogged:BatchBuilder[HNil,HNil] =
    BatchBuilder(logged = false)


}




trait BatchStatement[R,O] { self =>

  /** reads result received from executing batch statement **/
  def read(r:R)(rs:ResultSet, protocolVersion: ProtocolVersion):Either[Throwable, Option[O]]
  /** returns CQL representation of statements this batch operates on **/
  def statements:Seq[String]
  /** creates batch statement to be executed agains the C* **/
  def createStatement(statements:Seq[PreparedStatement], r:R, protocolVersion: ProtocolVersion):Either[Throwable, CBatchStatement]

  def mapIn[I](f: I => R):BatchStatement[I,O] = new BatchStatement[I,O] {
    def read(r: I)(rs: ResultSet, protocolVersion: ProtocolVersion): Either[Throwable, Option[O]] = self.read(f(r))(rs,protocolVersion)
    def createStatement(statements: Seq[PreparedStatement], r: I, protocolVersion: ProtocolVersion): Either[Throwable, CBatchStatement] =
      self.createStatement(statements,f(r), protocolVersion)
    def statements: Seq[String] = self.statements
  }

  def map[O2](f: O => O2):BatchStatement[R,O2] = new BatchStatement[R,O2] {
    def read(r: R)(rs: ResultSet, protocolVersion: ProtocolVersion): Either[Throwable, Option[O2]] =
      self.read(r)(rs,protocolVersion).right.map(_ map f)
    def createStatement(statements: Seq[PreparedStatement], r: R, protocolVersion: ProtocolVersion): Either[Throwable, CBatchStatement] =
      self.createStatement(statements,r,protocolVersion)
    def statements: Seq[String] = self.statements
  }

}

object BatchStatement {

  implicit class BatchStatementSyntaxQH[Q <: HList,R](val self: BatchStatement[Q,R]) extends AnyVal {
    /** converts batch to read queried parameters from `A` instead of `Q` **/
    def from[A](implicit G:Generic.Aux[A,Q]):BatchStatement[A,R] = self.mapIn(G.to)

    /** reads batch from supplied tuple **/
    def fromTuple[T](implicit T:ToHList.Aux[T,Q]):BatchStatement[T,R] = self.mapIn(T(_))


  }


  implicit class BatchStatementSyntaxRH[Q,R <: HList](val self: BatchStatement[Q,R]) extends AnyVal {

    /** converts result of batch to to `A` **/
    def as[A](implicit G:Generic.Aux[A,R]):BatchStatement[Q,A] = self.map(G.from)

    /** returns result as tuple **/
    def asTuple(implicit T: Tupler[R]):BatchStatement[Q,T.Out] = self.map(T(_))

  }

}



/**
  * Batch statements may result in more than one result returned when batch statement
  * Fails to execute. This, allows to correctly deserialize the result form the row
  * that corresponds to the result of the statement that failed to execute
  *
  * @tparam O
  */
trait BatchResultReader[O] { self =>
  /** yields to true, if the supplied row holds result for statement **/
  def readsFrom(row:Row, protocolVersion: ProtocolVersion):Boolean
  /** yields to result recovered from the row **/
  def read(row:Row, protocolVersion: ProtocolVersion):Either[Throwable,O]
  /** transoforms output type to O2 **/
  def map[O2](f: O => O2):BatchResultReader[O2] = new BatchResultReader[O2] {
    def readsFrom(row: Row, protocolVersion: ProtocolVersion): Boolean = self.readsFrom(row,protocolVersion)
    def read(row: Row, protocolVersion: ProtocolVersion): Either[Throwable, O2] = self.read(row,protocolVersion).right.map(f)
  }
}