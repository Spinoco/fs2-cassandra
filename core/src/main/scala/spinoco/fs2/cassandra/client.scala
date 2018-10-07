package spinoco.fs2.cassandra

import cats.effect.{Async, ContextShift, Resource}
import com.datastax.driver.core.{Cluster, QueryLogger}


object client {


  /**
    * Establish connection with cluster given configuration data passed in.
    * One the resulting stream terminates, then the cluster connection terminates too.
    */
  def cluster[F[_] : Async: ContextShift](config: Cluster.Builder, queryLogger: Option[QueryLogger] = None): Resource[F,CassandraCluster[F]] =
    CassandraCluster.instance(config, queryLogger)

  /**
    * Establish connection with the cluster and acquire session.
    * Session is cleared and connection with cluster terminated when stream completes.
    */
  def session[F[_] : Async: ContextShift](config: Cluster.Builder, queryLogger: Option[QueryLogger] = None): Resource[F,CassandraSession[F]] =
    cluster(config, queryLogger).flatMap(_.session)


}




