package spinoco.fs2.cassandra.system

/**
  * V3 Keyspace schema
  */
case class KeySpaceSchema (
  keyspace_name:String
  , durable_writes:Boolean
  , replication:Map[String,String]
 )


case class KeySpaceSchemaV2 (
   keyspace_name:String
   , durable_writes:Boolean
   , strategy_class:String
   , strategy_options:String
)
