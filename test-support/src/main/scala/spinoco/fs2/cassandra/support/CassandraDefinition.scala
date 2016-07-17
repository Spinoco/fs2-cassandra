package spinoco.fs2.cassandra.support

import shapeless.HNil
import spinoco.fs2.cassandra.Query
import spinoco.fs2.cassandra.system

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

  val `2.1`:CassandraDefinition =
    latest.copy(
      tag = "2.1"
      , allKeySpaceQuery = system.schema.queryAllKeySpacesV2.map(_.keyspace_name)
    )

  val `2.2`:CassandraDefinition =
    latest.copy(
      tag = "2.2"
      , allKeySpaceQuery = system.schema.queryAllKeySpacesV2.map(_.keyspace_name)
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

  lazy val All:Seq[CassandraDefinition] = Seq(
    `2.1`, `2.2`, `3.0`, `3.5`, `3.7`
  )


  implicit class CassandraDefinitionSnytax (val self: CassandraDefinition) extends AnyVal {
    def isV3Compatible:Boolean = {
      self == `3.0` || self == `3.5` || self == `3.7`
    }
  }

}
