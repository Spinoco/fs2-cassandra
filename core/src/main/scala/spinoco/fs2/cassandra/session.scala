package spinoco.fs2.cassandra

import cats.{Applicative, Monad}
import cats.data.OptionT
import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.core.{CqlSession, CqlSessionBuilder, ProtocolVersion}
import fs2._
import shapeless.HNil
import spinoco.fs2.cassandra.CassandraSession.impl.SessionState
import spinoco.fs2.cassandra.util.StatementHelper
import spinoco.fs2.cassandra.util.concurrent._

import java.time.Duration
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

}


object CassandraSession {

  /** given cluster this will create a single element stream with session **/
  def instance[F[_]
  : Async
  : ContextShift
  ](sessionBuilder: CqlSessionBuilder): Resource[F,CassandraSession[F]] = {

    def buildCqlSession : F[CqlSession] =
      Sync[F].suspend(sessionBuilder.buildAsync().toF)
      .flatTap(_ => shift)

    def closeSession(cqlSession: CqlSession): F[Unit] =
      Sync[F].suspend(cqlSession.closeAsync().toF) >> shift

    Resource.make(
      Sync[F].suspend(buildCqlSession).flatMap { cqlSession =>
        impl.mkSession[F](cqlSession, ???).map { cs => (cs, cqlSession) }
      }
    )({ case (_, cqlSession) => closeSession(cqlSession) })
    .flatMap { case (cs, _) => Resource.pure(cs) }
  }

  object impl {

//    val L = implicitly[Traverse[List]]
//
    implicit class ResultSetSyntax(val self:ResultSet) extends AnyVal {
      def drain:Vector[Row] = {
        val count = self.getAvailableWithoutFetching
        spinoco.fs2.cassandra.util.iterateN(self.iterator(), count)
      }
    }


    case class SessionState(
      cache:Map[String, PreparedStatement]
    )

    def mkSession[F[_]
    : Async
    : ContextShift
    : StatementHelper
    ](cqlSession: CqlSession, protocolVersion: ProtocolVersion): F[CassandraSession[F]] = Applicative[F].pure {
      new CassandraSession[F] {
        def create(ddl: SchemaDDL): F[Unit] =
          ddl.cqlStatement.toList.traverse_(executeCql(_))

        def migrateDDL(ddl: SchemaDDL): F[Seq[String]] =
          CassandraSession.migrateDDL(ddl)

        def execute[I, R](statement: DMLStatement[I, R], o: DMLOptions = Options.defaultDML)(i: I): F[R] =
          CassandraSession.executeDML(statement, o, i)

        def executeRaw[T <: Statement[T]](statement: T): F[AsyncResultSet] =
          Sync[F].suspend(cqlSession.executeAsync(statement).toF[F])

        def executeCql(cql: String, o: DMLOptions = Options.defaultDML): F[AsyncResultSet] =
          Sync[F].suspend(cqlSession.executeAsync(cql).toF)

        def query[Q, R](query: Query[Q, R], o:QueryOptions = Options.defaultQuery)(q: Q): Stream[F, R] =
          CassandraSession.queryStatement(query, o, q)

        def queryOne[Q, R](query: Query[Q, R], o: QueryOptions)(q: Q): F[Option[R]] =
          CassandraSession.queryOne(query, o, q)

        def queryCql(cql: String, o:QueryOptions = Options.defaultQuery): Stream[F, Row] =
          CassandraSession.queryRows(cqlStatement(cql, o))

        def queryStatement(boundStatement: BoundStatement): Stream[F, Row] =
          ??? // CassandraSession.queryRows(boundStatement, Options.defaultQuery)

        def page[Q, R](query: Query[Q, R], o:QueryOptions = Options.defaultQuery)(q: Q): Stream[F, Either[Option[PagingState], R]] =
          CassandraSession.pageQuery(query,o,q)

        def pageCql(cql: String, o:QueryOptions = Options.defaultQuery): Stream[F, Either[Option[PagingState], Row]] =
          CassandraSession.pageQueryRows(cqlStatement(cql, o))

        def pageStatement(boundStatement: BoundStatement): Stream[F, Either[Option[PagingState], Row]] =
          ??? // CassandraSession.pageQueryRows(boundStatement,Options.defaultQuery)

        def prepareCql(cql: String): F[PreparedStatement] =
          Sync[F].suspend(cqlSession.prepareAsync(cql).toF)

        def executeBatch[I, R](batch: BatchStatement[I, R], o: DMLOptions= Options.defaultDML)(i: I): F[Option[R]] =
          CassandraSession.executeBatch(batch,o,i)

        def bindStatement[I](statement: DMLStatement[I, _], o: DMLOptions)(i: I): F[BoundStatement] =
          ??? // mkStatement(statement,i).map { bs => Options.applyDMLOptions(bs,o)}

        def executeBatchRaw(statements: Seq[BoundStatement], logged: Boolean): F[AsyncResultSet] =
          CassandraSession.executeBatchRaw(statements,logged)
      }
    }

  }


  def executeBatchRaw[F[_]](statements: Seq[BoundStatement], logged: Boolean): F[AsyncResultSet] = { ???
//    val tpe = if (logged) CBatchStatement.Type.LOGGED else CBatchStatement.Type.UNLOGGED
//    val batch = new CBatchStatement(tpe)
//    batch.addAll(statements.asJava)
//    Sync[F].suspend(cs.executeAsync(batch))
  }

  def mkStatement[F[_], I](statement:CStatement[I], i:I): F[BoundStatement] = { ???
//    getOrRegisterStatement(statement.cqlStatement).map { ps =>
//      statement.fill(i,ps, protocolVersion)
//    }
  }


  def executeBatch[F[_], I, R](batch:BatchStatement[I, R], o:DMLOptions, i:I): F[Option[R]] = { ???
//    state.get.flatMap { s =>
//      val statements = batch.statements
//      def cacheNotPrepared =
//        statements.filterNot(s.cache.isDefinedAt).toList.traverse({ statement =>
//          Sync[F].suspend(cs.prepareAsync(statement)) map { statement -> _ }
//        }).flatMap { prepared =>
//          state.modify { s => val s1 = s.copy(cache = s.cache ++ prepared.toMap); (s1, s1) }
//        }
//
//      cacheNotPrepared.flatMap { cache =>
//        // here we have guaranteed that `cache` contains all statements, so we can just apply for them
//        val allStatements = statements.map(cache.cache.apply)
//        Sync[F].rethrow(Applicative[F].pure(batch.createStatement(allStatements,i,protocolVersion))).flatMap { statement =>
//          Sync[F].suspend(cs.executeAsync(Options.applyDMLOptions(statement,o))).flatMap { resultSet =>
//            Sync[F].rethrow(Applicative[F].pure(batch.read(i)(resultSet,protocolVersion)))
//          }}
//      }
//    }
  }

  // pages single query from supplied statement. Instead fetching next results,
  // this will return paging state on left, unless exhausted.
  def pageQueryRows[F[_], STMT <: Statement[STMT]](s: STMT):Stream[F,Either[Option[PagingState], Row]] = { ???
//    eval(Sync[F].suspend(cs.executeAsync(Options.applyQueryOptions(s,o)))).flatMap { rs =>
//      // paging state must be taken before the iteration starts
//      val paging:Option[PagingState] = {
//        if (rs.isExhausted) None
//        else Option(rs.getExecutionInfo.getPagingState)
//      }
//      Stream.emits(rs.drain.map(Right(_))) ++ Stream.emit(Left(paging))
//    }
  }


  def pageQuery[F[_], Q, R](query: Query[Q, R], o:QueryOptions, q:Q):Stream[F,Either[Option[PagingState],R]] = { ???
//    eval(mkStatement(query,q))
//      .flatMap { bs => _pageQueryRows(bs,o) }
//      .flatMap {
//        case Right(row) => query.read(row, protocolVersion).fold(Stream.raiseError[F],r => Stream.emit(Right(r)))
//        case Left(ps) => Stream.emit(Left(ps))
//      }
  }

  def queryRows[F[_], STMT <: Statement[STMT]](statement:STMT):Stream[F,Row] = { ???
//    def go(drained:ResultSet):Stream[F,Row] = {
//      if (drained.isExhausted) Stream.empty
//      else {
//        eval(Sync[F].suspend(drained.fetchMoreResults)).flatMap { rs =>
//          Stream.emits(rs.drain) ++ go(rs)
//        }
//      }
//    }
//    eval(Sync[F].suspend(cs.executeAsync(Options.applyQueryOptions(statement,o)))).flatMap { rs =>
//      Stream.emits(rs.drain) ++ go(rs)
//    }
  }

  def queryOne[F[_], Q,R](query: Query[Q, R], o: QueryOptions, q: Q): F[Option[R]] = { ???
//    getOrRegisterStatement(query.cqlStatement).flatMap { ps =>
//      val bs = Options.applyQueryOptions(query.fill(q,ps, protocolVersion),o)
//      bs.setFetchSize(1) // only one item we are interested in no need to fetch more
//      Sync[F].suspend(cs.executeAsync(bs)).flatMap {resultSet =>
//        Option(resultSet.one()) match {
//          case None => Applicative[F].pure(None)
//          case Some(row) => query.read(row,protocolVersion).fold(Sync[F].raiseError,r => Applicative[F].pure(Some(r)))
//        }
//      }
//    }
  }

  def queryStatement[F[_], Q, R](query: Query[Q, R], o:QueryOptions, q:Q):Stream[F,R] = { ???
//    eval(mkStatement(query,q))
//      .flatMap { bs => _queryRows(bs,o) }
//      .flatMap { row => query.read(row, protocolVersion).fold[Stream[F, R]](Stream.raiseError[F],Stream.emit) }
  }

  def executeDML[F[_]
  : Async
  : StatementHelper
    , I, R](statement: DMLStatement[I, R], o: DMLOptions, i: I, version: ProtocolVersion)(implicit cqlSession: CqlSession): F[R] = {
    StatementHelper[F].prepare(statement.cqlStatement).flatMap { ps =>
      val builder = statement.fill(i, ps, version) //todo: apply options
      Sync[F].suspend(cqlSession.executeAsync(builder.build()).toF)
    }

    mkStatement(statement,i).flatMap { bs =>
      Sync[F].suspend(cqlSession.executeAsync(bs).toF).flatMap { rs =>
        Sync[F].rethrow(Applicative[F].pure(statement.read(rs,protocolVersion)))
      }}
  }



  def migrateDDL[F[_]](ddl: SchemaDDL):F[Seq[String]] = { ???
//    ddl match {
//      case ks:KeySpace => Sync[F].delay { system.migrateKeySpace(ks, Option(cs.getCluster.getMetadata.getKeyspace(ks.name))) }
//      case t:Table[_,_,_,_] => Sync[F].delay {
//        val current = Option(cs.getCluster.getMetadata.getKeyspace(t.keySpaceName)).flatMap(km => Option(km.getTable(t.name)))
//        system.migrateTable(t, current)
//      }
//      case m: MaterializedView[_,_,_] => Sync[F].delay{
//        val current = Option(cs.getCluster.getMetadata.getKeyspace(m.keySpaceName)).flatMap(km => Option(km.getMaterializedView(m.name)))
//        system.migrateMaterializedView(m, current)
//      }
//    }
  }

  /**
    * Applies options to CQL statement
    * @param cql        CQl Statement
    * @param o          Options to apply to the statement
    * @return
    */
  def cqlStatement(cql: String, o: QueryOptions): SimpleStatement = {
    applyOptionsToStatement[SimpleStatement, SimpleStatementBuilder](SimpleStatement.builder(cql), o)
  }

  /**
    * Generalized form of applying options to any statement
    * @param builder  Builder to apply options to
    * @param o        Options to apply to the statement
    * @return
    */
  def applyOptionsToStatement[STMT <: Statement[STMT], BLDR <: StatementBuilder[BLDR, STMT]](builder: BLDR, o: QueryOptions): STMT = {
    def consistencyLevel(b: BLDR): BLDR = o.consistencyLevel.fold(b)(b.setConsistencyLevel)
    def tracing(b: BLDR): BLDR = o.tracing.fold(b)(b.setTracing)
    def executionProfileName (b: BLDR): BLDR = o.executionProfileName.fold(b)(b.setExecutionProfileName)
    def fetchSize(b: BLDR): BLDR = o.fetchSize.fold(b)(b.setPageSize)
    def readTimeout(b: BLDR): BLDR = o.timeout.fold(b)(dur => b.setTimeout(Duration.ofMillis(dur.toMillis)))
    def pagingState(b: BLDR): BLDR = o.pagingState.fold(b)(ps => b.setPagingState(ps.getRawPagingState))

    (
      consistencyLevel _ andThen
        tracing andThen
        executionProfileName andThen
        fetchSize andThen
        readTimeout andThen
        pagingState
      )(builder).build()

  }


}