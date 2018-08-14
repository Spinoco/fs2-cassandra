package spinoco.fs2.cassandra


import cats.effect.{Async, Timer}
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


  def apply[F[_]: Timer](config: Cluster.Builder, queryLogger: Option[QueryLogger])(implicit F:Async[F]):Stream[F,CassandraCluster[F]] = {
    bracket(F.delay { queryLogger.foldLeft(config.build())(_.register(_)) })(
      c => F.map{ F.suspend{ c.closeAsync() }}{_ => () }
    ).evalMap(impl.create[F])
  }


  object impl {

    def create[F[_]: Timer](cluster:Cluster)(implicit F:Async[F]):F[CassandraCluster[F]] = {
      F.delay {
        new CassandraCluster[F] {
          def session: Stream[F,CassandraSession[F]] = CassandraSession.apply(cluster)
        }
      }
    }

  }


}