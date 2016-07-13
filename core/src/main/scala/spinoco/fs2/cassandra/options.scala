package spinoco.fs2.cassandra


import com.datastax.driver.core.{ConsistencyLevel, PagingState, Statement}
import com.datastax.driver.core.policies.RetryPolicy

import scala.concurrent.duration.FiniteDuration

case class DMLOptions(
 consistencyLevel: Option[ConsistencyLevel]
 , serialConsistencyLevel: Option[ConsistencyLevel]
 , tracing: Option[Boolean]
 , retryPolicy: Option[RetryPolicy]
 , defaultTimeStamp: Option[Long]
 , idempotent: Option[Boolean]
) extends Options {

  /**
    * Sets the consistency level for the DML statement.
    */
  def withConsistencyLevel(level:ConsistencyLevel):DMLOptions =
    copy(consistencyLevel = Some(level))

  /**
    * Sets the serial consistency level for the query.
    *
    * The serial consistency level is only used by conditional updates ({@code INSERT}, {@code UPDATE}
    * or {@code DELETE} statements with an {@code IF} condition).
    * For those, the serial consistency level defines
    * the consistency level of the serial phase (or "paxos" phase) while the
    * normal consistency level defines the consistency for the "learn" phase, i.e. what
    * type of reads will be guaranteed to see the update right away. For instance, if
    * a conditional write has a regular consistency of QUORUM (and is successful), then a
    * QUORUM read is guaranteed to see that write. But if the regular consistency of that
    * write is ANY, then only a read with a consistency of SERIAL is guaranteed to see it
    * (even a read with consistency ALL is not guaranteed to be enough).
    *
    * The serial consistency can only be one of {@code ConsistencyLevel.SERIAL} or
    * {@code ConsistencyLevel.LOCAL_SERIAL}. While {@code ConsistencyLevel.SERIAL} guarantees full
    * linearizability (with other SERIAL updates), {@code ConsistencyLevel.LOCAL_SERIAL} only
    * guarantees it in the local data center.
    *
    * The serial consistency level is ignored for any query that is not a conditional
    * update (serial reads should use the regular consistency level for instance).
    *
    * @return
    */
  def withSerialConsistencyLevel(level:ConsistencyLevel):DMLOptions =
    copy(serialConsistencyLevel = Some(level))

  /**  Enables tracing for this DML statement **/
  def enableTracing:DMLOptions =
    copy(tracing = Some(true))

  /**
    * Sets the retry policy to use for this query.
    * The default retry policy, if this method is not called, is the one returned by
    * {@link com.datastax.driver.core.policies.Policies#getRetryPolicy} in the
    * cluster configuration. This method is thus only useful in case you want
    * to punctually override the default policy for this request.
    */
  def withRetryPolicy(policy:RetryPolicy):DMLOptions =
    copy(retryPolicy = Some(policy))

  /**
    * Sets the default timestamp for this query (in microseconds since the epoch).
    *
    * This feature is only available when version {@link ProtocolVersion#V3 V3} or
    * higher of the native protocol is in use. With earlier versions, calling this
    * method has no effect.
    *
    * The actual timestamp that will be used for this query is, in order of
    * preference:
    *
    *  the timestamp specified directly in the CQL query string (using the
    * {@code USING TIMESTAMP} syntax)
    *  the timestamp specified through this method, if different from
    * {@link Long#MIN_VALUE};
    *  the timestamp returned by the {@link TimestampGenerator} currently in use,
    * if different from {@link Long#MIN_VALUE}.
    *
    * If none of these apply, no timestamp will be sent with the query and Cassandra
    * will generate a server-side one (similar to the pre-V3 behavior).
    */
  def withDefaultTimeStamp(ts:Long):DMLOptions =
    copy(defaultTimeStamp = Some(ts))

  /**
    * Whether this statement is idempotent, i.e. whether it can be applied multiple times
    * without changing the result beyond the initial application.
    *
    * Overrides default behaviour, where statement is idempotnet when
    * - it updates counter
    * - it prepends / appends to the list
    * - uses function call
    *
    */
  def setIdempotent(idempotent:Boolean):DMLOptions =
    copy(idempotent = Some(idempotent))

}

case class QueryOptions(
 consistencyLevel: Option[ConsistencyLevel]
 , tracing: Option[Boolean]
 , retryPolicy: Option[RetryPolicy]
 , fetchSize: Option[Int]
 , readTimeout: Option[FiniteDuration]
 , pagingState: Option[PagingState]
) extends Options {

  /**
    * Sets the consistency level for the Query.
    */
  def withConsistencyLevel(level:ConsistencyLevel):QueryOptions =
    copy(consistencyLevel = Some(level))


  /**  Enables tracing for this Query **/
  def enableTracing:QueryOptions =
    copy(tracing = Some(true))

  /**
    * Sets the retry policy to use for this query.
    * The default retry policy, if this method is not called, is the one returned by
    * {@link com.datastax.driver.core.policies.Policies#getRetryPolicy} in the
    * cluster configuration. This method is thus only useful in case you want
    * to punctually override the default policy for this request.
    */
  def withRetryPolicy(policy:RetryPolicy):QueryOptions =
    copy(retryPolicy = Some(policy))

  /**
    * Overrides the default per-host read timeout ({@link SocketOptions#getReadTimeoutMillis()})
    * for this statement.
    * You should override this only for statements for which the coordinator may allow a longer server-side
    * timeout (for example aggregation queries).
    */
  def withReadTimeout(timeout:FiniteDuration):QueryOptions =
    copy(readTimeout = Some(timeout))

  /**
    * Sets paging state.
    * See java driver documentation about paging (http://datastax.github.io/java-driver/manual/paging/).
    * To be used with `page` methods on cassandra session.
    * @param page
    * @return
    */
  def startFrom(page:PagingState):QueryOptions =
    copy(pagingState = Some(page))

  /**
    * Sets the query fetch size.
    *
    * The fetch size controls how much resulting rows will be retrieved
    * simultaneously (the goal being to avoid loading too much results
    * in memory for queries yielding large results). Please note that
    * while value as low as 1 can be used, it is *highly* discouraged to
    * use such a low value in practice as it will yield very poor
    * performance. If in doubt, leaving the default is probably a good
    * idea.
    *
    */
  def withFetchSize(sz:Int):QueryOptions =
    copy(fetchSize = Some(sz))


}


sealed trait Options

object Options {

  val defaultDML:DMLOptions = DMLOptions(
    consistencyLevel = None
    , serialConsistencyLevel = None
    , tracing = None
    , retryPolicy = None
    , defaultTimeStamp = None
    , idempotent = None
  )

  val defaultQuery: QueryOptions = QueryOptions(
    consistencyLevel = None
    , tracing = None
    , retryPolicy = None
    , fetchSize = None
    , readTimeout = None
    , pagingState = None
  )

  /** sets starts of paging from given paging state **/
  def pageFrom(page:PagingState):QueryOptions =
    defaultQuery.startFrom(page)

  private[cassandra] def applyQueryOptions[S <: Statement](stmt:S, o:QueryOptions):S = {
    o.consistencyLevel.foreach(stmt.setConsistencyLevel)
    o.fetchSize.foreach(stmt.setFetchSize)
    o.pagingState.foreach(stmt.setPagingState)
    o.readTimeout.map(_.toMillis.toInt).foreach(stmt.setReadTimeoutMillis)
    o.retryPolicy.foreach(stmt.setRetryPolicy)
    o.tracing.foreach{ tracing => if (tracing) stmt.enableTracing() else stmt.disableTracing() }
    stmt
  }

  private[cassandra] def applyDMLOptions[S <: Statement](stmt:S, o:DMLOptions):S = {
    o.consistencyLevel.foreach(stmt.setConsistencyLevel)
    o.serialConsistencyLevel.foreach(stmt.setSerialConsistencyLevel)
    o.retryPolicy.foreach(stmt.setRetryPolicy)
    o.defaultTimeStamp.foreach(stmt.setDefaultTimestamp)
    o.idempotent.foreach(stmt.setIdempotent)
    o.tracing.foreach{ tracing => if (tracing) stmt.enableTracing() else stmt.disableTracing() }
    stmt
  }

}

