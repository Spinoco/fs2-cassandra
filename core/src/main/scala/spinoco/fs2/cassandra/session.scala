package spinoco.fs2.cassandra

import cats.{Applicative, Traverse}
import cats.implicits._
import cats.effect._
import cats.effect.concurrent.Ref
import com.datastax.driver.core._
import com.datastax.driver.core.{BatchStatement => CBatchStatement}
import fs2._
import fs2.Stream._
import shapeless.HNil
import simulacrum.typeclass

import scala.language.higherKinds
import scala.collection.JavaConverters._

@typeclass
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
  def executeRaw(statement:Statement):F[ResultSet]

  /** create bound statement that may be used later to form complex batch **/
  def bindStatement[I](statement: DMLStatement[I,_], o: DMLOptions = Options.defaultDML)(i:I):F[BoundStatement]

  /** Executes supplied batch statement. Returns None, if statement was applied correctly, Some(R) in case it was not applied **/
  def executeBatch[I,R](batch:BatchStatement[I,R], o: DMLOptions = Options.defaultDML)(i:I):F[Option[R]]

  /** raw variant of batch statement execution, allowing to join several bound statements together **/
  def executeBatchRaw(statements:Seq[BoundStatement],logged:Boolean):F[ResultSet]

  /** executes supplied DML statement specified by CQL expecting raw ResultSet as result **/
  def executeCql(cql:String, o: DMLOptions = Options.defaultDML):F[ResultSet]

  /** prepares supplied CQL statement **/
  def prepareCql(cql:String):F[PreparedStatement]

}


object CassandraSession {

  /** given cluster this will create a single element stream with session **/
  def instance[F[_] : Async : ContextShift](cluster:Cluster): Resource[F,CassandraSession[F]] = {
    Resource.make(
      Sync[F].suspend(cluster.connectAsync()).flatMap { cs =>
        impl.mkSession[F](cs, cluster.getConfiguration.getProtocolOptions.getProtocolVersion).map(cs -> _)
      }
    )({ case (cs, _) => Sync[F].suspend(toAsyncF(cs.closeAsync()).void) })
    .flatMap { case (_, cs) => Resource.pure(cs) }
  }

  object impl {

    val L = implicitly[Traverse[List]]

    implicit class ResultSetSyntax(val self:ResultSet) extends AnyVal {
      def drain:Vector[Row] = {
        val count = self.getAvailableWithoutFetching
        util.iterateN(self.iterator(), count)
      }
    }


    case class SessionState(
      cache:Map[String, PreparedStatement]
    )

    def mkSession[F[_]: Async : ContextShift](cs:Session, protocolVersion: ProtocolVersion):F[CassandraSession[F]] = {
      Ref.of[F, SessionState](SessionState(Map.empty)) map { state =>

        def executeDML[I, R](statement: DMLStatement[I, R],o: DMLOptions,i: I): F[R] =
         mkStatement(statement,i).flatMap { bs =>
         Sync[F].suspend(cs.executeAsync(Options.applyDMLOptions(bs,o))).flatMap { rs =>
            Sync[F].rethrow(Applicative[F].pure(statement.read(rs,protocolVersion)))
          }}


        def _queryRows(statement:Statement, o:QueryOptions):Stream[F,Row] = {
          def go(drained:ResultSet):Stream[F,Row] = {
            if (drained.isExhausted) Stream.empty
            else {
              eval(Sync[F].suspend(drained.fetchMoreResults)).flatMap { rs =>
                Stream.emits(rs.drain) ++ go(rs)
              }
            }
          }
          eval(Sync[F].suspend(cs.executeAsync(Options.applyQueryOptions(statement,o)))).flatMap { rs =>
            Stream.emits(rs.drain) ++ go(rs)
          }
        }

        def _queryStatement[Q,R](query: Query[Q, R], o:QueryOptions, q:Q):Stream[F,R] = {
          eval(mkStatement(query,q))
          .flatMap { bs => _queryRows(bs,o) }
          .flatMap { row => query.read(row, protocolVersion).fold[Stream[F, R]](Stream.raiseError[F],Stream.emit) }
        }

        def getOrRegisterStatement(cql:String):F[PreparedStatement] = {
         state.get.flatMap { s =>
            s.cache.get(cql) match {
              case Some(ps) => Applicative[F].pure(ps)
              case None =>
               Sync[F].suspend(cs.prepareAsync(cql)).flatMap { ps =>
                  state.update(s => s.copy(cache = s.cache + (cql -> ps))) as ps
                }
            }
          }
        }


        def mkStatement[I](statement:CStatement[I], i:I):F[BoundStatement] = {
          getOrRegisterStatement(statement.cqlStatement).map { ps =>
            statement.fill(i,ps, protocolVersion)
          }
        }

        // pages single query from supplied statement. Instead fetching next results,
        // this will return paging state on left, unless exhausted.
        def _pageQueryRows(s:Statement, o:QueryOptions):Stream[F,Either[Option[PagingState], Row]] = {
          eval(Sync[F].suspend(cs.executeAsync(Options.applyQueryOptions(s,o)))).flatMap { rs =>
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
            case Right(row) => query.read(row, protocolVersion).fold(Stream.raiseError[F],r => Stream.emit(Right(r)))
            case Left(ps) => Stream.emit(Left(ps))
          }
        }


        def _executeBatch[I,R](batch:BatchStatement[I,R], o:DMLOptions, i:I):F[Option[R]] = {
         state.get.flatMap { s =>
            val statements = batch.statements
            def cacheNotPrepared =
              statements.filterNot(s.cache.isDefinedAt).toList.traverse({ statement =>
                Sync[F].suspend(cs.prepareAsync(statement)) map { statement -> _ }
              }).flatMap { prepared =>
                state.modify { s => val s1 = s.copy(cache = s.cache ++ prepared.toMap); (s1, s1) }
              }

            cacheNotPrepared.flatMap { cache =>
             // here we have guaranteed that `cache` contains all statements, so we can just apply for them
             val allStatements = statements.map(cache.cache.apply)
             Sync[F].rethrow(Applicative[F].pure(batch.createStatement(allStatements,i,protocolVersion))).flatMap { statement =>
             Sync[F].suspend(cs.executeAsync(Options.applyDMLOptions(statement,o))).flatMap { resultSet =>
                 Sync[F].rethrow(Applicative[F].pure(batch.read(i)(resultSet,protocolVersion)))
             }}
            }
          }
        }


        def _migrateDDL(ddl: SchemaDDL):F[Seq[String]] = {
          ddl match {
            case ks:KeySpace => Sync[F].delay { system.migrateKeySpace(ks, Option(cs.getCluster.getMetadata.getKeyspace(ks.name))) }
            case t:Table[_,_,_,_] => Sync[F].delay {
              val current = Option(cs.getCluster.getMetadata.getKeyspace(t.keySpaceName)).flatMap(km => Option(km.getTable(t.name)))
              system.migrateTable(t, current)
            }
            case m: MaterializedView[_,_,_] => Sync[F].delay{
              val current = Option(cs.getCluster.getMetadata.getKeyspace(m.keySpaceName)).flatMap(km => Option(km.getMaterializedView(m.name)))
              system.migrateMaterializedView(m, current)
            }
          }
        }


        def _queryOne[Q,R](query: Query[Q, R], o: QueryOptions, q: Q): F[Option[R]] = {
         getOrRegisterStatement(query.cqlStatement).flatMap { ps =>
            val bs = Options.applyQueryOptions(query.fill(q,ps, protocolVersion),o)
            bs.setFetchSize(1) // only one item we are interested in no need to fetch more
            Sync[F].suspend(cs.executeAsync(bs)).flatMap {resultSet =>
              Option(resultSet.one()) match {
                case None => Applicative[F].pure(None)
                case Some(row) => query.read(row,protocolVersion).fold(Sync[F].raiseError,r => Applicative[F].pure(Some(r)))
              }
            }
          }
        }

        def _executeBatchRaw(statements: Seq[BoundStatement], logged: Boolean): F[ResultSet] = {
          val tpe = if (logged) CBatchStatement.Type.LOGGED else CBatchStatement.Type.UNLOGGED
          val batch = new CBatchStatement(tpe)
          batch.addAll(statements.asJava)
          Sync[F].suspend(cs.executeAsync(batch))
        }



        new CassandraSession[F] {
          def create(ddl: SchemaDDL): F[Unit] = ddl.cqlStatement.toList.traverse_(executeCql(_))
          def migrateDDL(ddl: SchemaDDL): F[Seq[String]] = _migrateDDL(ddl)
          def execute[I, R](statement: DMLStatement[I, R], o: DMLOptions = Options.defaultDML)(i: I): F[R] = executeDML(statement,o,i)
          def executeRaw(statement: Statement): F[ResultSet] = Sync[F].suspend(cs.executeAsync(statement))
          def executeCql(cql: String, o: DMLOptions = Options.defaultDML): F[ResultSet] = Sync[F].suspend(cs.executeAsync(cql))
          def query[Q, R](query: Query[Q, R], o:QueryOptions = Options.defaultQuery)(q: Q): Stream[F, R] = _queryStatement(query,o,q)
          def queryOne[Q, R](query: Query[Q, R], o: QueryOptions)(q: Q): F[Option[R]] = _queryOne(query,o,q)
          def queryCql(cql: String, o:QueryOptions = Options.defaultQuery): Stream[F, Row] = _queryRows(new SimpleStatement(cql),o)
          def queryStatement(boundStatement: BoundStatement): Stream[F, Row] = _queryRows(boundStatement,Options.defaultQuery)
          def page[Q, R](query: Query[Q, R], o:QueryOptions = Options.defaultQuery)(q: Q): Stream[F, Either[Option[PagingState], R]] = _pageQuery(query,o,q)
          def pageCql(cql: String, o:QueryOptions = Options.defaultQuery): Stream[F, Either[Option[PagingState], Row]] = _pageQueryRows(new SimpleStatement(cql), o)
          def pageStatement(boundStatement: BoundStatement): Stream[F, Either[Option[PagingState], Row]] = _pageQueryRows(boundStatement,Options.defaultQuery)
          def prepareCql(cql: String): F[PreparedStatement] = Sync[F].suspend(cs.prepareAsync(cql))
          def executeBatch[I, R](batch: BatchStatement[I, R], o: DMLOptions= Options.defaultDML)(i: I): F[Option[R]] = _executeBatch(batch,o,i)
          def bindStatement[I](statement: DMLStatement[I, _], o: DMLOptions)(i: I): F[BoundStatement] = mkStatement(statement,i).map { bs => Options.applyDMLOptions(bs,o)}
          def executeBatchRaw(statements: Seq[BoundStatement], logged: Boolean): F[ResultSet] = _executeBatchRaw(statements,logged)
        }
      }
    }

  }


}