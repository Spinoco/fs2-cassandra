package spinoco.fs2.cassandra

import spinoco.fs2.cassandra.support.CassandraDefinition

// TODO: Bug: First two integrations run fine. Third one fails with `com.datastax.oss.driver.api.core.AllNodesFailedException: Could not reach any contact point, make sure you've provided valid addresses`

class CassandraIntegration_4_1_Spec
  extends CommonCassandraSpec {
  override lazy val cassandra: CassandraDefinition = CassandraDefinition.`4.1`
}

class CassandraIntegration_3_9_Spec
  extends CommonCassandraSpec {
  override lazy val cassandra: CassandraDefinition = CassandraDefinition.`3.9`
}

class CassandraIntegration_3_7_Spec
  extends CommonCassandraSpec {
  override lazy val cassandra: CassandraDefinition = CassandraDefinition.`3.7`
}

class CassandraIntegration_3_5_Spec
  extends CommonCassandraSpec {
  override lazy val cassandra: CassandraDefinition = CassandraDefinition.`3.5`
}

class CassandraIntegration_3_0_Spec
  extends CommonCassandraSpec {
  override lazy val cassandra: CassandraDefinition = CassandraDefinition.`3.0`
}

// TODO: what to do about 2.2 and 2.1?

//class CassandraIntegration_2_2_Spec
//  extends CommonCassandraSpec {
//  override lazy val cassandra: CassandraDefinition = CassandraDefinition.`2.2`
//}

//class CassandraIntegration_2_1_Spec
//  extends CommonCassandraSpec {
//  override lazy val cassandra: CassandraDefinition = CassandraDefinition.`2.1`
//}