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
      .partition('keyspace_name)
      .createTable("keyspaces")

  val keySpacesV2 =
    system.table[KeySpaceSchemaV2]
      .partition('keyspace_name)
      .createTable("schema_keyspaces")

  val tables =
    system_schema.table[TableSchema]
      .partition('keyspace_name)
      .cluster('table_name)
      .createTable("tables")

  val columnFamiliesV2 =
    system.table[ColumnFamilySchemaV2]
      .partition('keyspace_name)
      .cluster('columnfamily_name)
      .createTable("schema_columnfamilies")

  val columns =
    system_schema.table[ColumnSchema]
      .partition('keyspace_name)
      .cluster('table_name)
      .cluster('column_name)
      .createTable("columns")

  val columnsV2 =
    system.table[ColumnSchemaV2]
      .partition('keyspace_name)
      .cluster('columnfamily_name)
      .cluster('column_name)
      .createTable("schema_columns")



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
