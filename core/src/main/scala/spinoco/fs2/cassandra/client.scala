package spinoco.fs2.cassandra

import cats.effect.Async
import com.datastax.driver.core.Cluster
import fs2._


object client {


  /**
    * Establish connection with cluster given configuration data passed in.
    * One the resulting stream terminates, then the cluster connection terminates too.
    */
  def cluster[F[_] : Async](config:Cluster.Builder):Stream[F,CassandraCluster[F]] =
    CassandraCluster(config)

  /**
    * Establish connection with the cluster and acquire session.
    * Session is cleared and connection with cluster terminated when stream completes.
    */
  def session[F[_] : Async](config:Cluster.Builder):Stream[F,CassandraSession[F]] =
    cluster(config).flatMap(_.session)


}




