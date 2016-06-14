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
  def query[Q,R](query:Query[Q,R], o:QueryOptions = Options.defaultQuery)(q:Q):Stream[F,R]

  /** Queries all elements. Requires query that accepts all results **/
  def queryAll[R](q:Query[HNil,R], o:QueryOptions = Options.defaultQuery):Stream[F,R] = query(q,o)(HNil)

  /** Builds a stream, that runs query specified by cql against C* when run **/
  def queryCql(cql:String, o:QueryOptions = Options.defaultQuery):Stream[F,Row]

  /** Builds a stream, that runs query specified by supplied `boundStatement` against C* when run **/
  def queryStatement(boundStatement: BoundStatement):Stream[F,Row]

  /**
    * Like query, but only teches single page and then returns on left paging state that can be reused to fetch more
    * Last element is guaranteed to be on Left, possibly empty, indicating end of paging.
    */
  def page[Q,R](query:Query[Q,R], o:QueryOptions = Options.defaultQuery)(q:Q):Stream[F,Either[Option[PagingState],R]]

  /**
    * Alias for paging w/o any query restrictions.
    * Last element is guaranteed to be on Left, possibly empty, indicating end of paging.
    */
  def pageAll[R](q:Query[HNil,R], o:QueryOptions = Options.defaultQuery):Stream[F,Either[Option[PagingState],R]]= page(q,o)(HNil)

  /**
    * Builds a paging stream, that runs query specified by cql against C* when run
    * Last element is guaranteed to be on Left, possibly empty, indicating end of paging.
    */
  def pageCql(cql:String, o:QueryOptions = Options.defaultQuery):Stream[F,Either[Option[PagingState],Row]]

  /**
    * Builds a paging stream, that runs query specified by supplied `boundStatement` against C* when run.
    * Last element is guaranteed to be on Left, possibly empty, indicating end of paging.
    */
  def pageStatement(boundStatement: BoundStatement):Stream[F,Either[Option[PagingState],Row]]

  /** Executes supplied DML statement (INSERT, UPDATE, DELETE).**/
  def execute[I,R](statement: DMLStatement[I,R], o: DMLOptions = Options.defaultDML)(i:I):F[R]

  /** Executes supplied batch statement **/
  def executeBatch[I,R](batch:BatchStatement[I,R], o: DMLOptions = Options.defaultDML)(i:I):F[R]

  /** executes supplied DML statement specified by CQL expecting raw ResultSet as result **/
  def executeCql(cql:String, o: DMLOptions = Options.defaultDML):F[ResultSet]

  /** prepares supplied CQL statement **/
  def prepareCql(cql:String):F[PreparedStatement]

}


object CassandraSession {




  def apply[F[_]](cluster:Cluster)(implicit F:Async[F]):Stream[F,CassandraSession[F]] = {
    Stream.bracket[F,Session,CassandraSession[F]](cluster.connectAsync())(
      {cs => Stream.eval(impl.mkSession(cs, cluster.getConfiguration.getProtocolOptions.getProtocolVersion))}
      , cs => F.map{ F.suspend { cs.closeAsync() } }(_ => ())
    )

  }




  object impl {

    implicit class ResultSetSyntax(val self:ResultSet) extends AnyVal {
      def drain:Vector[Row] = {
        val count = self.getAvailableWithoutFetching
        util.iterateN(self.iterator(), count)
      }
    }


    case class SessionState(
      cache:Map[String, PreparedStatement]
    )

    def mkSession[F[_]](cs:Session, protocolVersion: ProtocolVersion)(implicit F:Async[F]):F[CassandraSession[F]] = {
      F.map(F.refOf[SessionState](SessionState(Map.empty))) { state =>

        def executeDML[I, R](statement: DMLStatement[I, R],o: DMLOptions,i: I): F[R] = {
          F.bind(mkStatement(statement,i)) { bs =>
          F.bind(F.suspend(cs.executeAsync(Options.applyDMLOptions(bs,o)))) { rs =>
            statement.read(rs,protocolVersion).fold(F.fail, F.pure)
          }}
        }

        def _queryRows(statement:Statement, o:QueryOptions):Stream[F,Row] = {
          def go(drained:ResultSet):Stream[F,Row] = {
            if (drained.isExhausted) Stream.empty
            else {
              eval(F.suspend(drained.fetchMoreResults)).flatMap { rs =>
                Stream.emits(rs.drain) ++ go(rs)
              }
            }
          }
          eval(F.suspend(cs.executeAsync(Options.applyQueryOptions(statement,o)))).flatMap { rs =>
            Stream.emits(rs.drain) ++ go(rs)
          }
        }

        def _queryStatement[Q,R](query: Query[Q, R], o:QueryOptions, q:Q):Stream[F,R] = {
          eval(mkStatement(query,q))
          .flatMap { bs => _queryRows(bs,o) }
          .flatMap { row => query.read(row, protocolVersion).fold(Stream.fail,Stream.emit) }
        }

        def getOrRegisterStatement(cql:String):F[PreparedStatement] = {
          F.bind(F.get(state)) { s =>
            s.cache.get(cql) match {
              case Some(ps) => F.pure(ps)
              case None =>
                F.bind(F.suspend(cs.prepareAsync(cql))) { ps =>
                  F.map(F.modify(state)(s => s.copy(cache = s.cache + (cql -> ps)))) { _ => ps }
                }
            }
          }
        }


        def mkStatement[I](statement:CStatement[I], i:I):F[BoundStatement] = {
          F.map(getOrRegisterStatement(statement.cqlStatement)) { ps =>
            statement.fill(i,ps, protocolVersion)
          }
        }

        // pages single query from supplied statement. Instead fetching next results,
        // this will return paging state on left, unless exhausted.
        def _pageQueryRows(s:Statement, o:QueryOptions):Stream[F,Either[Option[PagingState], Row]] = {
          eval(F.suspend(cs.executeAsync(Options.applyQueryOptions(s,o)))).flatMap { rs =>
            // paging state must be taken before the iteration starts
            val paging:Option[PagingState] = {
              if (rs.isExhausted) None
              else Option(rs.getExecutionInfo.getPagingState)
            }
            Stream.emits(rs.drain.map(Right(_))) ++ Stream.emit(Left(paging))
          }
        }

        def _pageQuery[Q,R](query: Query[Q, R], o:QueryOptions, q:Q):Stream[F,Either[Option[PagingState],R]] = {
          eval(mkStatement(query,q))
          .flatMap { bs => _pageQueryRows(bs,o) }
          .flatMap {
            case Right(row) => query.read(row, protocolVersion).fold(Stream.fail,r => Stream.emit(Right(r)))
            case Left(ps) => Stream.emit(Left(ps))
          }
        }


        def _executeBatch[I,R](batch:BatchStatement[I,R], o:DMLOptions, i:I):F[R] = {
          F.bind(F.get(state)) { s =>
            val statements = batch.statements
            F.bind(
              F.traverse(statements.filterNot(s.cache.isDefinedAt)) { notPrepared =>
                F.map(F.suspend(cs.prepareAsync(notPrepared))) { notPrepared -> _ }
              }
            ) { stmts =>
              F.bind(F.modify(state){ s0 => s0.copy(cache = s0.cache ++ stmts.toMap) }) { c =>
                // here we have guaranteed that `now` contains all statements, so we can just apply for them
                val allStatements = statements.map(c.now.cache.apply)
                F.bind(batch.createStatement(allStatements,i,protocolVersion).fold(F.fail,F.pure)) { bs =>
                  F.bind(F.suspend(cs.executeAsync(Options.applyDMLOptions(bs,o)))) { rs =>
                    val rows = rs.drain
                    batch.read(i)(rows,protocolVersion).fold(F.fail,F.pure)
                  }
                }

              }
            }

          }
        }



        new CassandraSession[F] {
          def create(ddl: SchemaDDL): F[Unit] = F.map(executeCql(ddl.cqlStatement))(_ => ())
          def migrate(ddl: SchemaDDL): F[Boolean] = ???
          def diffDDL(ddl: SchemaDDL): F[Option[String]] = ???
          def execute[I, R](statement: DMLStatement[I, R], o: DMLOptions = Options.defaultDML)(i: I): F[R] = executeDML(statement,o,i)
          def executeCql(cql: String, o: DMLOptions = Options.defaultDML): F[ResultSet] = F.suspend(cs.executeAsync(cql))
          def query[Q, R](query: Query[Q, R], o:QueryOptions = Options.defaultQuery)(q: Q): Stream[F, R] = _queryStatement(query,o,q)
          def queryCql(cql: String, o:QueryOptions = Options.defaultQuery): Stream[F, Row] = _queryRows(new SimpleStatement(cql),o)
          def queryStatement(boundStatement: BoundStatement): Stream[F, Row] = _queryRows(boundStatement,Options.defaultQuery)
          def page[Q, R](query: Query[Q, R], o:QueryOptions = Options.defaultQuery)(q: Q): Stream[F, Either[Option[PagingState], R]] = _pageQuery(query,o,q)
          def pageCql(cql: String, o:QueryOptions = Options.defaultQuery): Stream[F, Either[Option[PagingState], Row]] = _pageQueryRows(new SimpleStatement(cql), o)
          def pageStatement(boundStatement: BoundStatement): Stream[F, Either[Option[PagingState], Row]] = _pageQueryRows(boundStatement,Options.defaultQuery)
          def prepareCql(cql: String): F[PreparedStatement] = F.suspend(cs.prepareAsync(cql))
          def executeBatch[I, R](batch: BatchStatement[I, R], o: DMLOptions= Options.defaultDML)(i: I): F[R] = _executeBatch(batch,o,i)
        }
      }
    }

  }


}