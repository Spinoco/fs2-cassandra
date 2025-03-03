package spinoco.fs2.cassandra

import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import cats.{Applicative, Traverse}
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, BatchType, BoundStatement, PagingState, PreparedStatement, Row, SimpleStatement, Statement, BatchStatement => CBatchStatement}
import com.datastax.oss.driver.api.core.{CqlSession, CqlSessionBuilder, ProtocolVersion, Version}
import fs2.Stream._
import fs2._
import shapeless.HNil
import spinoco.fs2.cassandra.util.AsyncResultSetSyntax.AsyncResultSetSyntaxes
import spinoco.fs2.cassandra.util.CompletionStageSyntax._
import spinoco.fs2.cassandra.util.CqlSessionSyntax._
import spinoco.fs2.cassandra.util.ToOptionSyntax._

import scala.collection.convert.ImplicitConversions._
import scala.language.higherKinds

trait CassandraSession[F[_]] {

  /** Creates given Schema object with DDL statement **/
  def create(ddl:SchemaDDL):F[Unit]

  /**
   * Compares supplied Schema object from actual version in C*, yielding in CQL statement that needs to be run to
   * update schema.
   */
  def migrateDDL(ddl:SchemaDDL):F[Seq[String]]

  /** Builds a stream, that runs query against C* when run **/
  def query[Q,R](query:Query[Q,R], o:QueryOptions = Options.defaultQuery)(q:Q):Stream[F,R]


  /** Queries all elements. Requires query that accepts all results **/
  def queryAll[R](q:Query[HNil,R], o:QueryOptions = Options.defaultQuery):Stream[F,R] = query(q,o)(HNil)

  /** queries only single result. This does nto guarantee there is only single result, but will always yield only in single `R` **/
  def queryOne[Q,R](query:Query[Q,R], o:QueryOptions = Options.defaultQuery)(q:Q):F[Option[R]]

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


  /** executes raw statement from the underlying C* driver **/
  def executeRaw[T <: Statement[T]](statement: T): F[AsyncResultSet]

  /** create bound statement that may be used later to form complex batch **/
  def bindStatement[I](statement: DMLStatement[I,_], o: DMLOptions = Options.defaultDML)(i:I):F[BoundStatement]

  /** Executes supplied batch statement. Returns None, if statement was applied correctly, Some(R) in case it was not applied **/
  def executeBatch[I,R](batch:BatchStatement[I,R], o: DMLOptions = Options.defaultDML)(i:I):F[Option[R]]

  /** raw variant of batch statement execution, allowing to join several bound statements together **/
  def executeBatchRaw(statements:Seq[BoundStatement],logged:Boolean):F[AsyncResultSet]

  /** executes supplied DML statement specified by CQL expecting raw ResultSet as result **/
  def executeCql(cql:String, o: DMLOptions = Options.defaultDML):F[AsyncResultSet]

  /** prepares supplied CQL statement **/
  def prepareCql(cql:String):F[PreparedStatement]

  def minCassandraVersion(): Option[Version]
}


object CassandraSession {

  /** given cluster this will create a single element stream with session **/
  def instance[F[_]
  : Async
  : ContextShift
  ](sessionBuilder: CqlSessionBuilder): Resource[F,CassandraSession[F]] = {



    def buildCqlSession : F[CqlSession] =
      Sync[F].suspend(sessionBuilder.buildAsync().toF)
        .flatTap(_ => implicitly[ContextShift[F]].shift)

    def closeSession(cqlSession: CqlSession): F[Unit] =
      Sync[F].suspend(cqlSession.closeAsync().toF) >> implicitly[ContextShift[F]].shift

    Resource.make(
        Sync[F].suspend(buildCqlSession).flatMap { cqlSession =>
          impl.mkSession[F](cqlSession, cqlSession.getContext.getProtocolVersion).map { cs => (cs, cqlSession) }
        }
      )({ case (_, cqlSession) => closeSession(cqlSession) })
      .flatMap { case (cs, _) => Resource.pure(cs) }
  }

  object impl {

    val L = implicitly[Traverse[List]]

    case class SessionState( cache:Map[String, PreparedStatement] )

    def mkSession[F[_]: Async : ContextShift](cqlSession: CqlSession, protocolVersion: ProtocolVersion):F[CassandraSession[F]] = {

      Ref.of[F, SessionState](SessionState(Map.empty)) map { state =>

        def executeDML[I, R](statement: DMLStatement[I, R],o: DMLOptions,i: I): F[R] = {
          mkStatement(statement, i).flatMap { bs =>
            Sync[F].suspend {
              cqlSession.executeAsync(Options.applyDMLOptions(bs, o)).toF
            }.flatMap { rs =>
              // TODO: can these effect wrappers be simplified?
              Sync[F].rethrow(Applicative[F].pure{
                 Stream.emit(rs)
                  .flatMap { ars => ars.toStream[IO] }
                  .compile.last
                  .map(statement.readResult(_, protocolVersion))
                  .unsafeRunSync()
              })
            }
          }
        }

        def _queryRows[T <: Statement[T]](statement: T, o:QueryOptions):Stream[F,Row] = {
         val asyncResult = Async[F].suspend(cqlSession.executeAsync(Options.applyQueryOptions(statement,o)).toF)
          Stream.eval(asyncResult).flatMap { ars => ars.toStream[F] }
        }

        def _queryStatement[Q,R](query: Query[Q, R], o:QueryOptions, q:Q):Stream[F,R] = {
          eval(mkStatement(query,q))
            .flatMap { bs =>
              _queryRows(bs,o)
            }
            .flatMap { row => query.read(row, protocolVersion).fold[Stream[F, R]](Stream.raiseError[F],Stream.emit) }
        }

        def getOrRegisterStatement(cql:String):F[PreparedStatement] = {
          state.get.flatMap { s =>
            s.cache.get(cql) match {
              case Some(ps) => Applicative[F].pure(ps)
              case None =>
                Sync[F].suspend(cqlSession.prepareAsync(cql).toF).flatMap { ps =>
                  state.update(s => s.copy(cache = s.cache + (cql -> ps))) as ps
                }
            }
          }
        }

        def cassandraVersion(): Option[Version] = {
          cqlSession.getMetadata.getNodes.values().iterator().toSeq
            .map(_.getCassandraVersion)
            .reduceOption((a, b) => if (a.compareTo(b) < 0) a else b)
        }

        def mkStatement[I](statement:CStatement[I], i:I):F[BoundStatement] = {
          getOrRegisterStatement(statement.cqlStatement).map { ps =>
            statement.fill(i,ps, protocolVersion)
          }
        }

        // pages single query from supplied statement. Instead fetching next results,
        // this will return paging state on left, unless exhausted.
        def _pageQueryRows[S <: Statement[S]](s: S, o:QueryOptions):Stream[F,Either[Option[PagingState], Row]] = {
          Stream.eval(cqlSession.executeAsync(Options.applyQueryOptions(s,o)).toF).flatMap { rs =>
            // paging state must be taken before the iteration starts
            val paging:Option[PagingState] = {
              if (rs.remaining == 0 && !rs.hasMorePages) None
              else Option(rs.getExecutionInfo.getSafePagingState)
            }
            val rows = rs.drain.map(Right(_))
            Stream.emits(rows) ++ Stream.emit(Left(paging))
          }
        }

        def _pageQuery[Q,R](query: Query[Q, R], o:QueryOptions, q:Q):Stream[F,Either[Option[PagingState],R]] = {
          eval(mkStatement(query,q))
            .flatMap { bs => _pageQueryRows(bs,o) }
            .flatMap {
              case Right(row) => query.read(row, protocolVersion).fold(Stream.raiseError[F],r => Stream.emit(Right(r)))
              case Left(ps) => Stream.emit(Left(ps))
            }
        }

        def _executeBatch[I,R](batch:BatchStatement[I,R], o:DMLOptions, i:I):F[Option[R]] = {
          state.get.flatMap { s =>
            val statements = batch.statements
            def cacheNotPrepared =
              statements.filterNot(s.cache.isDefinedAt).toList.traverse({ statement =>
                Sync[F].suspend(cqlSession.prepareAsync(statement).toF) map { statement -> _ }
              }).flatMap { prepared =>
                state.modify { s => val s1 = s.copy(cache = s.cache ++ prepared.toMap); (s1, s1) }
              }

            cacheNotPrepared.flatMap { cache =>
              // here we have guaranteed that `cache` contains all statements, so we can just apply for them
              val allStatements = statements.map(cache.cache.apply)
              Sync[F].rethrow(Applicative[F].pure(batch.createStatement(allStatements,i,protocolVersion))).flatMap { statement =>
                Sync[F].suspend(cqlSession.executeAsync(Options.applyDMLOptions(statement,o)).toF).flatMap { rs =>
                  // TODO: can these effect wrappers be simplified? (e.g., direct stream to F conversion)
                  Sync[F].rethrow(Applicative[F].pure{
                    val wasApplied = rs.wasApplied()
                    val all = rs.toStream[IO]
                      .compile.toVector
                      .unsafeRunSync()

                    if (wasApplied) Right(None)
                    else batch.readResult(i)(all,protocolVersion).right.map(Some(_))
                  })
                }
              }
            }
          }
        }

        def _migrateDDL(ddl: SchemaDDL):F[Seq[String]] = {
          ddl match {
            case ks:KeySpace => Sync[F].delay {
              val metadata = cqlSession.getKeyspaceMetadata(ks.name)
              system.migrateKeySpace(ks, metadata)
            }
            case t:Table[_,_,_,_] => Sync[F].delay {
              val current = cqlSession.getKeyspaceMetadata(t.keySpaceName).flatMap(km => km.getTable(t.name).toOption)
              system.migrateTable(t, current)
            }
          }
        }


        def _queryOne[Q,R](query: Query[Q, R], o: QueryOptions, q: Q): F[Option[R]] = {
          getOrRegisterStatement(query.cqlStatement).flatMap { ps =>
            val bs = Options
              .applyQueryOptions(query.fill(q,ps, protocolVersion),o)
              .setPageSize(1) // only one item we are interested in no need to fetch more
            Sync[F].suspend(cqlSession.executeAsync(bs).toF).flatMap {resultSet =>
              Option(resultSet.one()) match {
                case None => Applicative[F].pure(None)
                case Some(row) => query.read(row,protocolVersion).fold(Sync[F].raiseError,r => Applicative[F].pure(Some(r)))
              }
            }
          }
        }

        def _executeBatchRaw(statements: Seq[BoundStatement], logged: Boolean): F[AsyncResultSet] = {
          val tpe = if (logged) BatchType.LOGGED else BatchType.UNLOGGED
          val batch = CBatchStatement
            .builder(tpe)
            .addStatements(statements: _*)
            .build()
          Sync[F].suspend(cqlSession.executeAsync(batch).toF)
        }



        new CassandraSession[F] {
          def create(ddl: SchemaDDL): F[Unit] = ddl.cqlStatement.toList.traverse_(executeCql(_))
          def migrateDDL(ddl: SchemaDDL): F[Seq[String]] = _migrateDDL(ddl)
          def execute[I, R](statement: DMLStatement[I, R], o: DMLOptions = Options.defaultDML)(i: I): F[R] = executeDML(statement, o, i)
          def executeRaw[T <: Statement[T]](statement: T): F[AsyncResultSet] = Async[F].suspend( cqlSession.executeAsync(statement).toF )
          def executeCql(cql: String, o: DMLOptions = Options.defaultDML): F[AsyncResultSet] = Async[F].suspend( cqlSession.executeAsync(cql).toF )
          def query[Q, R](query: Query[Q, R], o:QueryOptions = Options.defaultQuery)(q: Q): Stream[F, R] = _queryStatement(query,o,q)
          def queryOne[Q, R](query: Query[Q, R], o: QueryOptions)(q: Q): F[Option[R]] = _queryOne(query,o,q)
          def queryCql(cql: String, o:QueryOptions = Options.defaultQuery): Stream[F, Row] = _queryRows(SimpleStatement.builder(cql).build,o)
          def queryStatement(boundStatement: BoundStatement): Stream[F, Row] = _queryRows(boundStatement,Options.defaultQuery)
          def page[Q, R](query: Query[Q, R], o:QueryOptions = Options.defaultQuery)(q: Q): Stream[F, Either[Option[PagingState], R]] = _pageQuery(query,o,q)
          def pageCql(cql: String, o:QueryOptions = Options.defaultQuery): Stream[F, Either[Option[PagingState], Row]] = _pageQueryRows(SimpleStatement.builder(cql).build, o)
          def pageStatement(boundStatement: BoundStatement): Stream[F, Either[Option[PagingState], Row]] = _pageQueryRows(boundStatement,Options.defaultQuery)
          def prepareCql(cql: String): F[PreparedStatement] = Async[F].suspend( cqlSession.prepareAsync(cql).toF )
          def executeBatch[I, R](batch: BatchStatement[I, R], o: DMLOptions= Options.defaultDML)(i: I): F[Option[R]] = _executeBatch(batch,o,i)
          def bindStatement[I](statement: DMLStatement[I, _], o: DMLOptions)(i: I): F[BoundStatement] = mkStatement(statement,i).map { bs => Options.applyDMLOptions(bs,o)}
          def executeBatchRaw(statements: Seq[BoundStatement], logged: Boolean): F[AsyncResultSet] = _executeBatchRaw(statements,logged)
          def minCassandraVersion(): Option[Version] = cassandraVersion()
        }
      }
    }

  }


}