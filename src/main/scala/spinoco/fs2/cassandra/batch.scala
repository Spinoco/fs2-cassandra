package spinoco.fs2.cassandra

import com.datastax.driver.core.{PreparedStatement, ProtocolVersion, Row, BatchStatement => CBatchStatement}
import shapeless.HNil
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




trait BatchStatement[R,O] {

  /** reads result received from executing batch statement **/
  def read(r:R)(rows:Seq[Row], protocolVersion: ProtocolVersion):Either[Throwable, O]
  /** returns CQL representation of statements this batch operates on **/
  def statements:Seq[String]
  /** creates batch statement to be executed agains the C* **/
  def createStatement(statements:Seq[PreparedStatement], r:R, protocolVersion: ProtocolVersion):Either[Throwable, CBatchStatement]


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