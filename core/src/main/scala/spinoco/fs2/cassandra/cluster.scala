package spinoco.fs2.cassandra


import cats.effect.{Async, ContextShift, Resource, Sync}


trait CassandraCluster[F[_]] {

  /**
    * Acquire single session, that can be used to access c*.
    * Note that this emits only once, and session is closed when the resulting process terminates
    * @return
    */
  def session: Resource[F,CassandraSession[F]]

}

object CassandraCluster {
  def wrap[F[_] : Async : ContextShift](cs: CassandraSession[F]): CassandraCluster[F] = {
      new CassandraCluster[F] {
        def session: Resource[F, CassandraSession[F]] = Resource.make(Sync[F].pure(cs))(_ => Sync[F].unit)
      }
  }
}

