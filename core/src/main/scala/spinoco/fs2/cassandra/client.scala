package spinoco.fs2.cassandra

import cats.effect.{Async, ContextShift}
import com.datastax.driver.core.{Cluster, QueryLogger}
import fs2._


object client {


  /**
    * Establish connection with cluster given configuration data passed in.
    * One the resulting stream terminates, then the cluster connection terminates too.
    */
  def cluster[F[_] : Async: ContextShift](config: Cluster.Builder, queryLogger: Option[QueryLogger] = None):Stream[F,CassandraCluster[F]] =
    CassandraCluster(config, queryLogger)

  /**
    * Establish connection with the cluster and acquire session.
    * Session is cleared and connection with cluster terminated when stream completes.
    */
  def session[F[_] : Async: ContextShift](config: Cluster.Builder, queryLogger: Option[QueryLogger] = None):Stream[F,CassandraSession[F]] =
    cluster(config, queryLogger).flatMap(_.session)


}




