package spinoco.fs2.cassandra

import spinoco.fs2.cassandra.support.CassandraDefinition


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

class CassandraIntegration_2_2_Spec
  extends CommonCassandraSpec {
  override lazy val cassandra: CassandraDefinition = CassandraDefinition.`2.2`
}

class CassandraIntegration_2_1_Spec
  extends CommonCassandraSpec {
  override lazy val cassandra: CassandraDefinition = CassandraDefinition.`2.1`
}