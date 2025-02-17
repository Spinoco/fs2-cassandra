package spinoco.fs2.cassandra.util

import cats.data.OptionT
import cats.effect.concurrent.Ref
import cats.effect.{Async, Sync}
import cats.syntax.all._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import spinoco.fs2.cassandra.util.CompletionStageSyntax._

trait StatementCache[F[_]] {

  /**
    * Prepares the cql statement if not yet in cache.
    * If the statement is in the cache, it is not prepared ogain
    * @param cql  Cql statement to prepare
    * @return
    */
  def prepare(cql: String): F[PreparedStatement]

}


object StatementCache {

  @inline def apply[F[_]](implicit instance: StatementCache[F]): StatementCache[F] = instance

  /**
    * Creates cache
    * @param cqlSession CQl Session used to create prepared statements
    * @tparam F
    * @return
    */
  def mk[F[_]: Async](cqlSession: CqlSession): F[StatementCache[F]] = {
    Ref.of[F, Map[String, PreparedStatement]](Map.empty).map { ref =>
      new StatementCache[F] {
        def prepare(cql: String): F[PreparedStatement] =
          StatementCache.prepare(ref, cqlSession, cql)
      }
    }
  }

  /**
    * New prepared statement is either taken from the cache or new is created
    * @param ref          Cache of prepared statements
    * @param cqlSession   Session to create prepared statement
    * @param cql          Cql to use
    * @tparam F
    * @return
    */
  def prepare[F[_]: Async](
    ref: Ref[F, Map[String, PreparedStatement]]
    , cqlSession: CqlSession
    , cql: String
  ): F[PreparedStatement] = {
    OptionT(ref.get.map(_.get(cql))).getOrElseF {
      Sync[F].suspend(cqlSession.prepareAsync(cql).toF).flatMap { ps =>
        ref.update(_ + (cql -> ps)) as ps
      }
    }
  }


}