package spinoco.fs2.cassandra

import spinoco.fs2.cassandra.support.CassandraDefinition


class PlayGroundSpec  extends InsertSpec {

  override lazy val cassandra: CassandraDefinition = CassandraDefinition.`2.2`

//  override lazy val clearContainers: Boolean = false
//  override lazy val startContainers: Boolean = false

  //override def preserveKeySpace(s: String): Boolean = true

}
