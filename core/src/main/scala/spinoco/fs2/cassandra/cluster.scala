package spinoco.fs2.cassandra


import cats.effect.{Async, ContextShift}
import cats.syntax.functor._
import com.datastax.driver.core.{Cluster, QueryLogger}
import fs2._
import fs2.Stream._


trait CassandraCluster[F[_]] {

  /**
    * Acquire single session, that can be used to access c*.
    * Note that this emits only once, and session is closed when the resulting process terminates
    * @return
    */
  def session: Stream[F,CassandraSession[F]]


}


object CassandraCluster {


  def apply[F[_]: ContextShift](config: Cluster.Builder, queryLogger: Option[QueryLogger])(implicit F:Async[F]):Stream[F,CassandraCluster[F]] = {
    bracket(F.delay { queryLogger.foldLeft(config.build())(_.register(_)) })(
      c => F.suspend(c.closeAsync()).void
    ).evalMap(impl.create[F])
  }


  object impl {

    def create[F[_]: ContextShift](cluster:Cluster)(implicit F:Async[F]):F[CassandraCluster[F]] = {
      F.delay {
        new CassandraCluster[F] {
          def session: Stream[F,CassandraSession[F]] = CassandraSession.apply(cluster)
        }
      }
    }

  }


}