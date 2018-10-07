package spinoco.fs2.cassandra


import cats.implicits._
import cats.effect.{Async, ContextShift, Resource, Sync}
import com.datastax.driver.core.{Cluster, QueryLogger}


trait CassandraCluster[F[_]] {

  /**
    * Acquire single session, that can be used to access c*.
    * Note that this emits only once, and session is closed when the resulting process terminates
    * @return
    */
  def session: Resource[F,CassandraSession[F]]


}


object CassandraCluster {

  @inline def apply[F[_]](implicit instance: CassandraCluster[F]): CassandraCluster[F] = instance

  def instance[F[_]: Async: ContextShift](config: Cluster.Builder, queryLogger: Option[QueryLogger]):Resource[F,CassandraCluster[F]] = {
    Resource.make[F, (Cluster, CassandraCluster[F])](
      Sync[F].delay { queryLogger.foldLeft(config.build())(_.register(_)) }.flatMap { cluster =>
        impl.create[F](cluster).map (cluster -> _)
      }
    )({ case (cluster, _ ) => Sync[F].suspend{ toAsyncF(cluster.closeAsync()).void } })
    .flatMap { case (_, cluster) => Resource.pure(cluster) }
  }


  object impl {

    def create[F[_]: Async: ContextShift](cluster:Cluster):F[CassandraCluster[F]] = {
      Sync[F].pure {
        new CassandraCluster[F] {
          def session: Resource[F,CassandraSession[F]] = CassandraSession.instance(cluster)
        }
      }
    }

  }


}