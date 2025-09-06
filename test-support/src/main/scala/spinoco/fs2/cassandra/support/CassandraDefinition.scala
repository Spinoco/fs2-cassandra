package spinoco.fs2.cassandra.support

import shapeless.HNil
import spinoco.fs2.cassandra.{Query, system}

/**
  * Created by pach on 08/06/16.
  */
case class CassandraDefinition(
  imageBase:String
  , tag:String
  , allKeySpaceQuery:Query[HNil, String]
) {

  lazy val dockerImageUrl:String = s"$imageBase:$tag"


}

object CassandraDefinition {

  val latest = CassandraDefinition(
    imageBase = "cassandra"
    , tag = "latest"
    , allKeySpaceQuery = system.schema.queryAllKeySpaces.map(_.keyspace_name)
  )


  val `3.0`:CassandraDefinition =
    latest.copy(
      tag = "3.0"
      , allKeySpaceQuery = system.schema.queryAllKeySpaces.map(_.keyspace_name)
    )

  val `3.5`:CassandraDefinition =
    latest.copy(
      tag = "3.5"
      , allKeySpaceQuery = system.schema.queryAllKeySpaces.map(_.keyspace_name)
    )

  val `3.7`:CassandraDefinition =
    latest.copy(
      tag = "3.7"
      , allKeySpaceQuery = system.schema.queryAllKeySpaces.map(_.keyspace_name)
    )

  val `3.9`:CassandraDefinition =
    latest.copy(
      tag = "3.9"
      , allKeySpaceQuery = system.schema.queryAllKeySpaces.map(_.keyspace_name)
    )

  val `3.11`:CassandraDefinition =
    latest.copy(
      tag = "3.11"
      , allKeySpaceQuery = system.schema.queryAllKeySpaces.map(_.keyspace_name)
    )

  val `4.0`:CassandraDefinition =
    latest.copy(
      tag = "4.0"
      , allKeySpaceQuery = system.schema.queryAllKeySpaces.map(_.keyspace_name)
    )

  val `4.1`:CassandraDefinition =
    latest.copy(
      tag = "4.1"
      , allKeySpaceQuery = system.schema.queryAllKeySpaces.map(_.keyspace_name)
    )

  val `5.0`:CassandraDefinition =
    latest.copy(
      tag = "5.0"
      , allKeySpaceQuery = system.schema.queryAllKeySpaces.map(_.keyspace_name)
    )



  lazy val All:Seq[CassandraDefinition] = Seq(
    `3.0`, `3.5`, `3.7`, `3.9`, `3.11`, `4.0`, `4.1`, `5.0`
  )


  implicit class CassandraDefinitionSnytax (val self: CassandraDefinition) extends AnyVal {
    def isV3Compatible:Boolean = true // All supported versions are 3.x+
  }

  def fromVersion(version: String): CassandraDefinition = version match {
    case "3.0" => `3.0`
    case "3.5" => `3.5`
    case "3.7" => `3.7`
    case "3.9" => `3.9`
    case "3.11" => `3.11`
    case "4.0" => `4.0`
    case "4.1" => `4.1`
    case "5.0" => `5.0`
    case v => throw new IllegalArgumentException(s"Unsupported Cassandra version: $v. Supported versions: ${All.map(_.tag).mkString(", ")}")
  }

}
