package spinoco.fs2.cassandra

import spinoco.fs2.cassandra.support.DockerCassandra


trait CommonCassandraSpec
  extends UpdateSpec
    with InsertSpec
    with SchemaSpec
    with CrudSpec
    with DeleteSpec
    with PagingSpec
    with BatchSpec
    with DockerCassandra

