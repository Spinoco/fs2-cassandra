package spinoco.fs2.cassandra


import com.datastax.driver.core._
import fs2._
import fs2.Async
import fs2.Stream._
import shapeless.HNil

import scala.language.higherKinds


trait CassandraSession[F[_]] {

  /** Creates given Schema object with DDL statement **/
  def create(ddl:SchemaDDL):F[Unit]

  /**
    * Migrates supplied Schema object from actual version in C*.
    * Yields false, if migration was not necessary and true, if it performed successfully.
    *
    */
  def migrate(ddl:SchemaDDL):F[Boolean]

  /**
    * Compares supplied Schema object from actual version in C*, yielding in CQL statement that needs to be run to
    * update schema.
    */
  def diffDDL(ddl:SchemaDDL):F[Option[String]]

  /** Builds a stream, that runs query against C* when run **/
  def query[Q,R](query:Query[Q,R])(q:Q):Stream[F,R]

  /** Queries all elements. Requires query that accepts all results **/
  def queryAll[R](q:Query[HNil,R]):Stream[F,R] = query(q)(HNil)

  /** Builds a stream, that runs query specified by cql against C* when run **/
  def query(cql:String):Stream[F,Row]

  /** Builds a stream, that runs query specified by supplied `boundStatement` against C* when run **/
  def query(boundStatement: BoundStatement):Stream[F,Row]

  /** Executes supplied DML statement (INSERT, UPDATE, DELETE).**/
  def execute[I,R](statement: DMLStatement[I,R])(i:I):F[R]

  /** executes supplied DML statement specified by CQL expecting raw ResultSet as result **/
  def execute(cql:String):F[ResultSet]

  /** prepares supplied CQL statement **/
  def prepare(cql:String):F[PreparedStatement]

}


object CassandraSession {




  def apply[F[_]](cluster:Cluster)(implicit F:Async[F]):Stream[F,CassandraSession[F]] = {
    Stream.bracket[F,Session,CassandraSession[F]](cluster.connectAsync())(
      {cs => Stream.eval(impl.mkSession(cs, cluster.getConfiguration.getProtocolOptions.getProtocolVersion))}
      , cs => F.map{ F.suspend { cs.closeAsync() } }(_ => ())
    )

  }


  object impl {

    case class SessionState(
      cache:Map[String, PreparedStatement]
    )

    def mkSession[F[_]](cs:Session, protocolVersion: ProtocolVersion)(implicit F:Async[F]):F[CassandraSession[F]] = {
      F.map(F.refOf[SessionState](SessionState(Map.empty))) { state =>


        def execute1[I](statement:CStatement[I],i:I):F[ResultSet] = {
          F.bind(F.get(state)) { s =>
            s.cache.get(statement.cqlStatement) match {
              case Some(ps) =>
                F.suspend(cs.executeAsync(statement.fill(i,ps, protocolVersion)))
                //F.delay(cs.executeAsync(statement.fill(i,ps, protocolVersion)).get())

              case None =>
                F.bind(cs.prepareAsync(statement.cqlStatement)) { ps =>
                F.bind(F.modify(state)(s => s.copy(cache = s.cache + (statement.cqlStatement -> ps)))) { _ =>
                  F.suspend(cs.executeAsync(statement.fill(i,ps, protocolVersion)))
                }}
            }
          }
        }


        def executeDML[I, R](statement: DMLStatement[I, R],i: I): F[R] = {
          F.bind(execute1(statement, i)) { rs =>
            statement.read(rs,protocolVersion).fold(F.fail, F.pure)
          }
        }

        def executeQuery[Q,R](query: Query[Q, R], q:Q):Stream[F,R] = {
          def emitAndNext(processed:ResultSet):Stream[F,Row] = {
            if (processed.isExhausted) Stream.empty
            else {
              eval(processed.fetchMoreResults():F[ResultSet]).flatMap { rs =>
                Stream.emits(util.iterateN(rs.iterator(), rs.getAvailableWithoutFetching)) ++ emitAndNext(rs)
              }
            }
          }
          eval(execute1(query,q))
          .flatMap { rs =>
            Stream.emits(util.iterateN(rs.iterator(), rs.getAvailableWithoutFetching)) ++ emitAndNext(rs)
          }
          .flatMap { row =>
            query.read(row, protocolVersion).fold(Stream.fail, Stream.emit)
          }
        }



        new CassandraSession[F] {
          def create(ddl: SchemaDDL): F[Unit] = F.map(execute(ddl.cqlStatement))(_ => ())
          def migrate(ddl: SchemaDDL): F[Boolean] = ???
          def diffDDL(ddl: SchemaDDL): F[Option[String]] = ???
          def execute[I, R](statement: DMLStatement[I, R])(i: I): F[R] = executeDML(statement,i)
          def execute(cql: String): F[ResultSet] = cs.executeAsync(cql)
          def query[Q, R](query: Query[Q, R])(q: Q): Stream[F, R] = executeQuery(query,q)
          def query(cql: String): Stream[F, Row] = ???
          def query(boundStatement: BoundStatement): Stream[F, Row] = ???
          def prepare(cql: String): F[PreparedStatement] = cs.prepareAsync(cql)
        }
      }
    }

  }


}