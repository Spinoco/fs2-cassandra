package spinoco.fs2.cassandra

import fs2.Stream._
import fs2._
import spinoco.fs2.cassandra.sample.SimpleTableRow


class SchemaSpec extends SchemaSupport {

  "Create Schema" - {
    val ks = KeySpace("spec_ks")

    "create simple KeySpace " in withCluster { c =>

      val query = system.schema.queryAllKeySpaces.map(_.keyspace_name)

      val result =
        Stream.resource(c.session).flatMap { cs => eval_(cs.create(ks)) ++ cs.queryAll(query) }
        .compile.toVector.unsafeRunSync()

      result should contain ("spec_ks")
    }

    "create SimpleTable" in withCluster { c =>

      val table = ks.table[SimpleTableRow].partition('intColumn).build("simple_table")

      val query = system.schema.queryAllTables.map(t => t.keyspace_name -> t.table_name)

      val result =
      Stream.resource(c.session)
      .flatMap { cs =>
        eval_(cs.create(ks)) ++
          eval_(cs.create(table)) ++
          cs.queryAll(query)
      }
      .compile.toVector.unsafeRunSync()

      result should contain ("spec_ks" -> "simple_table")
    }


    "create SimpleTable with compound primary key" in withCluster { c =>
      val table =
        ks.table[SimpleTableRow]
          .partition('intColumn)
          .partition('longColumn)
          .build("simple_compound_pk_table")

      val query = system.schema.queryAllColumns.map(c => (c.keyspace_name, c.table_name, c.column_name, c.kind))


      val result =
        Stream.resource(c.session)
        .flatMap { cs =>
          eval_(cs.create(ks)) ++
            eval_(cs.create(table)) ++
            cs.queryAll(query)
        }
        .compile.toVector.unsafeRunSync()

      val columnSpecs =
      result
      .collect{ case ("spec_ks","simple_compound_pk_table",c,kind) => c -> kind }

      columnSpecs should contain allOf(
        "intcolumn" -> "partition_key"
        , "longcolumn" -> "partition_key"
      )

    }


    "create SimpleTable with compound cluster key" in withCluster { c =>
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
          Stream.resource(c.session)
            .flatMap { cs =>
              eval_(cs.create(ks)) ++
                eval_(cs.create(table)) ++
                cs.queryAll(query)
            }
            .compile.toVector.unsafeRunSync()


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
}
