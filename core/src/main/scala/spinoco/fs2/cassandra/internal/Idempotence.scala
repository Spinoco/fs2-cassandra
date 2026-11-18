package spinoco.fs2.cassandra.internal

import com.datastax.oss.driver.api.core.cql.{BatchStatement, Statement}

import scala.jdk.CollectionConverters._

/**
  * Idempotence of the statements built by this library.
  *
  * The driver retries a request on another node (e.g. when the connection drops before the response arrives)
  * only when the request is idempotent. A request that leaves idempotence unset uses the driver's
  * `basic.request.default-idempotence` setting.
  */
object Idempotence {

  /**
    * Marks the statement as not idempotent when `nonIdempotent` is true,
    * otherwise leaves it unchanged so the driver's default applies.
    */
  def markNonIdempotent[S <: Statement[S]](statement: S, nonIdempotent: Boolean): S =
    if (nonIdempotent) statement.setIdempotent(false) else statement

  /**
    * Marks the batch as not idempotent if any of its statements is explicitly not idempotent.
    * The driver resolves idempotence of a batch only from the batch itself, not from its statements.
    */
  def ofBatch(batch: BatchStatement): BatchStatement = {
    val anyNonIdempotent = batch.iterator().asScala.exists(s => java.lang.Boolean.FALSE.equals(s.isIdempotent))
    markNonIdempotent(batch, anyNonIdempotent)
  }

}
