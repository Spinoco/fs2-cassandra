package spinoco.fs2.cassandra.system

import java.nio.ByteBuffer


/**
  * C* 3+ schema for columns
  */
case class ColumnSchema(
  keyspace_name: String
  , table_name: String
  , column_name: String
  , clustering_order: String
  , column_name_bytes: ByteBuffer
  , kind:String
  , position: Int
  , `type`: String
)

/**
  * C* v2 columns
  */
case class ColumnSchemaV2(
  keyspace_name :String
  , columnfamily_name :String
  , column_name : String
  , component_index : Option[Int]
  , index_name :Option[String]
  , index_options :Option[String]
  , index_type :Option[String]
  , `type` :String
  , validator :String
)
