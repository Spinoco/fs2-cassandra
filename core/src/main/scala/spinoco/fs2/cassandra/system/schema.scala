package spinoco.fs2.cassandra.system

import spinoco.fs2.cassandra.KeySpace

/**
  * System schema tables and objects
  */
object schema {


  val system_schema = KeySpace("system_schema")
  val system = KeySpace("system")


  val keySpaces =
    system_schema.table[KeySpaceSchema]
      .partition(Symbol("keyspace_name"))
      .build("keyspaces")

  val keySpacesV2 =
    system.table[KeySpaceSchemaV2]
      .partition(Symbol("keyspace_name"))
      .build("schema_keyspaces")

  val tables =
    system_schema.table[TableSchema]
      .partition(Symbol("keyspace_name"))
      .cluster(Symbol("table_name"))
      .build("tables")

  val columnFamiliesV2 =
    system.table[ColumnFamilySchemaV2]
      .partition(Symbol("keyspace_name"))
      .cluster(Symbol("columnfamily_name"))
      .build("schema_columnfamilies")

  val columns =
    system_schema.table[ColumnSchema]
      .partition(Symbol("keyspace_name"))
      .cluster(Symbol("table_name"))
      .cluster(Symbol("column_name"))
      .build("columns")

  val columnsV2 =
    system.table[ColumnSchemaV2]
      .partition(Symbol("keyspace_name"))
      .cluster(Symbol("columnfamily_name"))
      .cluster(Symbol("column_name"))
      .build("schema_columns")



  ////////////////////////////////////////////////////////////
  // Queries

  val queryAllKeySpaces =
    keySpaces.query.all.build.as[KeySpaceSchema]

  val queryAllKeySpacesV2 =
    keySpacesV2.query.all.build.as[KeySpaceSchemaV2]

  val queryAllTables =
    tables.query.all.build.as[TableSchema]

  val queryAllColumnFamiliesV2 =
    columnFamiliesV2.query.all.build.as[ColumnFamilySchemaV2]

  val queryAllColumns =
    columns.query.all.build.as[ColumnSchema]

  val queryAllColumnsV2 =
    columnsV2.query.all.build.as[ColumnSchemaV2]




}
