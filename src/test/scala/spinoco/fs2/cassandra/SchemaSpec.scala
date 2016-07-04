package spinoco.fs2.cassandra

import fs2.Stream._
import spinoco.fs2.cassandra.sample.SimpleTableRow


trait SchemaSpec extends SchemaSupport {

  s"Create Schema (${cassandra.tag})" - {
    val ks = KeySpace("spec_ks")

    "create simple KeySpace " in withCluster { c =>

      val query =
        if (! cassandra.isV3Compatible) {
          system.schema.queryAllKeySpacesV2.map(_.keyspace_name)
        } else system.schema.queryAllKeySpaces.map(_.keyspace_name)

      val result =
        c.session.flatMap { cs => eval_(cs.create(ks)) ++ cs.queryAll(query) }
        .runLog.unsafeRun

      result should contain ("spec_ks")
    }

    "create SimpleTable" in withCluster { c =>

      val table = ks.table[SimpleTableRow].partition('intColumn).build("simple_table")

      val query =
        if (! cassandra.isV3Compatible) {
          system.schema.queryAllColumnFamiliesV2.map {cf => cf.keyspace_name -> cf.columnfamily_name}
        } else {
          system.schema.queryAllTables.map(t => t.keyspace_name -> t.table_name)
        }

      val result =
      c.session
      .flatMap { cs =>
        eval_(cs.create(ks)) ++
          eval_(cs.create(table)) ++
          cs.queryAll(query)
      }
      .runLog.unsafeRun

      result should contain ("spec_ks" -> "simple_table")
    }


    "create SimpleTable with compound primary key" in withCluster { c =>
      val table =
        ks.table[SimpleTableRow]
          .partition('intColumn)
          .partition('longColumn)
          .build("simple_compound_pk_table")

      val query =
        if (! cassandra.isV3Compatible) {
          system.schema.queryAllColumnsV2.map(c => (c.keyspace_name, c.columnfamily_name, c.column_name, c.`type`))
        } else {
          system.schema.queryAllColumns.map(c => (c.keyspace_name, c.table_name, c.column_name, c.kind))
        }


      val result =
        c.session
        .flatMap { cs =>
          eval_(cs.create(ks)) ++
            eval_(cs.create(table)) ++
            cs.queryAll(query)
        }
        .runLog.unsafeRun

      val columnSpecs =
      result
      .collect{ case ("spec_ks","simple_compound_pk_table",c,kind) => c -> kind }

      columnSpecs should contain allOf(
        "intcolumn" -> "partition_key"
        , "longcolumn" -> "partition_key"
      )

    }


    "create SimpleTable with compound cluster key (V3)" in withCluster { c =>
      if (! cassandra.isV3Compatible) {
        succeed // this tests will only pass in 3.0+
      } else {
        val table =
          ks.table[SimpleTableRow]
            .partition('intColumn)
            .partition('longColumn)
            .cluster('stringColumn)
            .cluster('asciiColumn)
            .build("simple_compound_ck_table")

        val query =
          system.schema.queryAllColumns.map(c => (c.keyspace_name, c.table_name, c.column_name, c.kind))


        val result =
          c.session
            .flatMap { cs =>
              eval_(cs.create(ks)) ++
                eval_(cs.create(table)) ++
                cs.queryAll(query)
            }
            .runLog.unsafeRun


        val columnSpecs =
          result
            .collect { case ("spec_ks", "simple_compound_ck_table", c, kind) => c -> kind }


        columnSpecs should contain allOf(
          "intcolumn" -> "partition_key"
          , "longcolumn" -> "partition_key"
          , "stringcolumn" -> "clustering"
          , "asciicolumn" -> "clustering"
          )

      }
    }

    "create SimpleTable with compound cluster key (V2)" in withCluster { c =>
      if (cassandra.isV3Compatible) {
        succeed // this tests will only pass in 2.1/2.2
      } else {
        val table =
          ks.table[SimpleTableRow]
            .partition('intColumn)
            .partition('longColumn)
            .cluster('stringColumn)
            .cluster('asciiColumn)
            .build("simple_compound_ck_table")

        val query =
          system.schema.queryAllColumnsV2
          .map(c => (c.keyspace_name, c.columnfamily_name, c.column_name, c.`type`))


        val result =
          c.session
            .flatMap { cs =>
              eval_(cs.create(ks)) ++
                eval_(cs.create(table)) ++
                cs.queryAll(query)
            }
            .runLog.unsafeRun


        val columnSpecs =
          result
            .collect{ case ("spec_ks","simple_compound_ck_table",c,kind) => c -> kind }


        columnSpecs should contain allOf(
          "intcolumn" -> "partition_key"
          , "longcolumn" -> "partition_key"
          , "stringcolumn" -> "clustering_key"
          , "asciicolumn" -> "clustering_key"
          )
      }


    }



  }
}
