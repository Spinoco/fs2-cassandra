package spinoco.fs2.cassandra


import com.datastax.driver.core._
import com.datastax.driver.core.{BatchStatement => CBatchStatement}
import fs2._
import fs2.util.{Async, Traverse}
import fs2.Stream._
import shapeless.HNil

import scala.language.higherKinds
import scala.collection.JavaConverters._


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
  def apply[F[_]](cluster:Cluster)(implicit F:Async[F]):Stream[F,CassandraSession[F]] = {
    Stream.bracket[F,Session,CassandraSession[F]](cluster.connectAsync())(
      {cs => Stream.eval(impl.mkSession(cs, cluster.getConfiguration.getProtocolOptions.getProtocolVersion))}
      , cs => F.map{ F.suspend { cs.closeAsync() } }(_ => ())
    )

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

    def mkSession[F[_]](cs:Session, protocolVersion: ProtocolVersion)(implicit F:Async[F]):F[CassandraSession[F]] = {
      F.map(F.refOf[SessionState](SessionState(Map.empty))) { state =>

        def executeDML[I, R](statement: DMLStatement[I, R],o: DMLOptions,i: I): F[R] = {
         F.flatMap(mkStatement(statement,i)) { bs =>
         F.flatMap(F.suspend(cs.executeAsync(Options.applyDMLOptions(bs,o)))) { rs =>
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
         F.flatMap(state.get) { s =>
            s.cache.get(cql) match {
              case Some(ps) => F.pure(ps)
              case None =>
               F.flatMap(F.suspend(cs.prepareAsync(cql))) { ps =>
                  F.map(state.modify(s => s.copy(cache = s.cache + (cql -> ps)))) { _ => ps }
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


        def _executeBatch[I,R](batch:BatchStatement[I,R], o:DMLOptions, i:I):F[Option[R]] = {

         F.flatMap(state.get) { s =>
            val statements = batch.statements
           F.flatMap(
             L.traverse(statements.filterNot(s.cache.isDefinedAt).toList) { notPrepared =>
                F.map(F.suspend(cs.prepareAsync(notPrepared))) { notPrepared -> _ }
              }
            ) { stmts =>
             F.flatMap(state.modify{ s0 => s0.copy(cache = s0.cache ++ stmts.toMap) }) { c =>
                // here we have guaranteed that `now` contains all statements, so we can just apply for them
                val allStatements = statements.map(c.now.cache.apply)
               F.flatMap(batch.createStatement(allStatements,i,protocolVersion).fold(F.fail,F.pure)) { bs =>
                 F.flatMap(F.suspend(cs.executeAsync(Options.applyDMLOptions(bs,o)))) { rs =>
                    batch.read(i)(rs,protocolVersion).fold(F.fail,F.pure)
                  }
                }

              }
            }

          }
        }


        def _migrateDDL(ddl: SchemaDDL):F[Seq[String]] = {
          ddl match {
            case ks:KeySpace => F.delay { system.migrateKeySpace(ks, Option(cs.getCluster.getMetadata.getKeyspace(ks.name))) }
            case t:Table[_,_,_,_] => F.delay {
              val current = Option(cs.getCluster.getMetadata.getKeyspace(t.keySpaceName)).flatMap(km => Option(km.getTable(t.name)))
              system.migrateTable(t, current)
            }
            case m: MaterializedView[_,_,_] => F.delay{
              val current = Option(cs.getCluster.getMetadata.getKeyspace(m.keySpaceName)).flatMap(km => Option(km.getMaterializedView(m.name)))
              system.migrateMaterializedView(m, current)
            }
          }
        }


        def _queryOne[Q,R](query: Query[Q, R], o: QueryOptions, q: Q): F[Option[R]] = {
         F.flatMap(getOrRegisterStatement(query.cqlStatement)) { ps =>
            val bs = Options.applyQueryOptions(query.fill(q,ps, protocolVersion),o)
            bs.setFetchSize(1) // only one item we are interested in no need to fetch more
           F.flatMap(F.suspend(cs.executeAsync(bs))) { rs =>
              Option(rs.one()) match {
                case None => F.pure(None)
                case Some(row) => query.read(row,protocolVersion).fold(F.fail,r => F.pure(Some(r)))
              }
            }
          }
        }

        def _executeBatchRaw(statements: Seq[BoundStatement], logged: Boolean): F[ResultSet] = {
          val tpe = if (logged) CBatchStatement.Type.LOGGED else CBatchStatement.Type.UNLOGGED
          val batch = new CBatchStatement(tpe)
          batch.addAll(statements.asJava)
          F.suspend(cs.executeAsync(batch))
        }



        new CassandraSession[F] {
          def create(ddl: SchemaDDL): F[Unit] = F.map(L.traverse(ddl.cqlStatement.toList)(executeCql(_)))(_ => ())
          def migrateDDL(ddl: SchemaDDL): F[Seq[String]] = _migrateDDL(ddl)
          def execute[I, R](statement: DMLStatement[I, R], o: DMLOptions = Options.defaultDML)(i: I): F[R] = executeDML(statement,o,i)
          def executeRaw(statement: Statement): F[ResultSet] = F.suspend(cs.executeAsync(statement))
          def executeCql(cql: String, o: DMLOptions = Options.defaultDML): F[ResultSet] = F.suspend(cs.executeAsync(cql))
          def query[Q, R](query: Query[Q, R], o:QueryOptions = Options.defaultQuery)(q: Q): Stream[F, R] = _queryStatement(query,o,q)
          def queryOne[Q, R](query: Query[Q, R], o: QueryOptions)(q: Q): F[Option[R]] = _queryOne(query,o,q)
          def queryCql(cql: String, o:QueryOptions = Options.defaultQuery): Stream[F, Row] = _queryRows(new SimpleStatement(cql),o)
          def queryStatement(boundStatement: BoundStatement): Stream[F, Row] = _queryRows(boundStatement,Options.defaultQuery)
          def page[Q, R](query: Query[Q, R], o:QueryOptions = Options.defaultQuery)(q: Q): Stream[F, Either[Option[PagingState], R]] = _pageQuery(query,o,q)
          def pageCql(cql: String, o:QueryOptions = Options.defaultQuery): Stream[F, Either[Option[PagingState], Row]] = _pageQueryRows(new SimpleStatement(cql), o)
          def pageStatement(boundStatement: BoundStatement): Stream[F, Either[Option[PagingState], Row]] = _pageQueryRows(boundStatement,Options.defaultQuery)
          def prepareCql(cql: String): F[PreparedStatement] = F.suspend(cs.prepareAsync(cql))
          def executeBatch[I, R](batch: BatchStatement[I, R], o: DMLOptions= Options.defaultDML)(i: I): F[Option[R]] = _executeBatch(batch,o,i)
          def bindStatement[I](statement: DMLStatement[I, _], o: DMLOptions)(i: I): F[BoundStatement] = F.map(mkStatement(statement,i)) { bs => Options.applyDMLOptions(bs,o)}
          def executeBatchRaw(statements: Seq[BoundStatement], logged: Boolean): F[ResultSet] = _executeBatchRaw(statements,logged)
        }
      }
    }

  }


}