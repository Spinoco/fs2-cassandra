package spinoco.fs2.cassandra


import cats.effect.Async
import com.datastax.driver.core.Cluster
import fs2._
import fs2.Stream._


trait CassandraCluster[F[_]] {

  /**
    * Acquire single session, that can be used to access c*.
    * Note that this emits only once, and session is closed when the resulting process terminates
    * @return
    */
  def session:Stream[F,CassandraSession[F]]


}


object CassandraCluster {


  def apply[F[_]](config:Cluster.Builder)(implicit F:Async[F]):Stream[F,CassandraCluster[F]] = {
    bracket(F.delay { config.build() })(
      c => F.map{ F.suspend{ c.closeAsync() }}{_ => () }
    ).evalMap(impl.create[F])
  }


  object impl {

    def create[F[_]](cluster:Cluster)(implicit F:Async[F]):F[CassandraCluster[F]] = {
      F.delay {
        new CassandraCluster[F] {
          def session: Stream[F,CassandraSession[F]] = CassandraSession.apply(cluster)
        }
      }
    }

  }


}