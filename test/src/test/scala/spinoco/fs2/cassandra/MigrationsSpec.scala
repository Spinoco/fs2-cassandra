package spinoco.fs2.cassandra

import spinoco.fs2.cassandra.builder.SimilarityFunction
import spinoco.fs2.cassandra.sample.VectorTableRow

class MigrationsSpec extends SchemaSupport {

  case class FooTable1(intColumn:Int, longColumn:Long, strColumn:String )
  case class FooTable2(intColumn:Int, longColumn1:Long, strColumn:Int )

  val table1 = ks.table[FooTable1].partition(Symbol("intColumn")).build("foo1")
  val table2 = ks.table[FooTable2].partition(Symbol("intColumn")).build("foo1")
  val table3 = ks.table[FooTable1].partition(Symbol("intColumn")).cluster(Symbol("longColumn")).build("foo1")


  "Migrations" - {

    "will emit create for KeySpace that does not exists" in withSession { cs =>

      cs.migrateDDL(ks).unsafeRunSync() shouldBe Seq(
        "CREATE KEYSPACE crud_ks WITH REPLICATION = {'class':'org.apache.cassandra.locator.SimpleStrategy','replication_factor':'1'} AND DURABLE_WRITES = true "
        )

    }

    "will not emit create//alter if KeySpace is same" in withSession { cs =>

      cs.create(ks).unsafeRunSync()

      cs.migrateDDL(ks).unsafeRunSync() shouldBe Nil

    }

    "will update KeySpace DURABLE_WRITES" in withSession { cs =>

      cs.create(ks).unsafeRunSync()

      val migrate = cs.migrateDDL(ks.withDurableWrites(durable = false)).unsafeRunSync()

      migrate shouldBe Seq("ALTER KEYSPACE crud_ks WITH DURABLE_WRITES = false")

      migrate.foreach(cs.executeCql(_).unsafeRunSync())

      cs.migrateDDL(ks.withDurableWrites(durable = false)).unsafeRunSync() shouldBe Nil

    }

    "will update KeySpace REPLICATION" in withSession { cs =>

      cs.create(ks).unsafeRunSync()

      val migrate = cs.migrateDDL(ks.copy(strategyOptions = Seq("replication_factor" -> "2"))).unsafeRunSync()

      migrate shouldBe Seq("ALTER KEYSPACE crud_ks WITH REPLICATION = {'replication_factor':'2','class':'org.apache.cassandra.locator.SimpleStrategy'}")

      migrate.foreach(cs.executeCql(_).unsafeRunSync())

      cs.migrateDDL(ks.copy(strategyOptions = Seq("replication_factor" -> "2"))).unsafeRunSync() shouldBe Nil

    }


    "will emit create table if table does not exists" in withSession { cs =>
      cs.create(ks).unsafeRunSync()

      val migrate = cs.migrateDDL(table1).unsafeRunSync()

      migrate shouldBe Seq(
        "CREATE TABLE crud_ks.foo1 (intColumn int,longColumn bigint,strColumn text, PRIMARY KEY ((intColumn)))"
      )
    }

    "will not emit update if schema is same" in withSession { cs =>
      cs.create(ks).unsafeRunSync()
      cs.create(table1).unsafeRunSync()

      val migrate = cs.migrateDDL(table1).unsafeRunSync()

      migrate shouldBe Nil

    }

    "will update column defs if they have changed" in withSession { cs =>
      cs.create(ks).unsafeRunSync()
      cs.create(table1).unsafeRunSync()

      val migrate = cs.migrateDDL(table2).unsafeRunSync()

      val expectedDrops = Seq(
        "ALTER TABLE crud_ks.foo1 DROP longcolumn"
        ,"ALTER TABLE crud_ks.foo1 DROP strcolumn"
      )
      val expectedAdds = Seq(
        "ALTER TABLE crud_ks.foo1 ADD longColumn1 bigint"
        , "ALTER TABLE crud_ks.foo1 ADD strColumn int"
      )

      /** Drops have to go fist. */
      migrate.slice(0,2).toSet shouldBe expectedDrops.toSet
      /** Adds after. */
      migrate.slice(2,4).toSet shouldBe expectedAdds.toSet

      /** Check we have checked everything. */
      migrate.toSet shouldBe (expectedDrops ++ expectedAdds).toSet
    }


    "will drop and create table if primary key has changed" in withSession { cs =>
      cs.create(ks).unsafeRunSync()
      cs.create(table1).unsafeRunSync()

      val migrate = cs.migrateDDL(table3).unsafeRunSync()

      migrate shouldBe Seq(
        "DROP TABLE crud_ks.foo1"
        , "CREATE TABLE crud_ks.foo1 (intColumn int,longColumn bigint,strColumn text, PRIMARY KEY ((intColumn),longColumn))"
      )
    }

    "will add SAI index if missing" in withSessionFor(_.startsWith("5")) { cs =>
      val tableNoIdx = ks.table[FooTable1].partition(Symbol("intColumn")).build("foo_sai")
      val tableWithIdx = ks.table[FooTable1].partition(Symbol("intColumn"))
        .indexBySAI(Symbol("strColumn"), "str_sai_idx")
        .build("foo_sai")

      cs.create(ks).unsafeRunSync()
      cs.create(tableNoIdx).unsafeRunSync()

      val migrate = cs.migrateDDL(tableWithIdx).unsafeRunSync()
      migrate shouldBe Seq(
        "CREATE CUSTOM INDEX str_sai_idx ON crud_ks.foo_sai (strColumn) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'"
      )

      // apply migration
      migrate.foreach(cs.executeCql(_).unsafeRunSync())

      // no further migration needed
      cs.migrateDDL(tableWithIdx).unsafeRunSync() shouldBe Nil
    }

    "will drop removed SAI index" in withSessionFor(_.startsWith("5")) { cs =>
      val tableWithIdx = ks.table[FooTable1].partition(Symbol("intColumn"))
        .indexBySAI(Symbol("strColumn"), "str_sai_idx2")
        .build("foo_sai2")
      val tableNoIdx = ks.table[FooTable1].partition(Symbol("intColumn")).build("foo_sai2")

      cs.create(ks).unsafeRunSync()
      cs.create(tableWithIdx).unsafeRunSync()

      val migrate = cs.migrateDDL(tableNoIdx).unsafeRunSync()
      migrate shouldBe Seq("DROP INDEX crud_ks.str_sai_idx2")

      migrate.foreach(cs.executeCql(_).unsafeRunSync())
      cs.migrateDDL(tableNoIdx).unsafeRunSync() shouldBe Nil
    }

    "will not emit alter for unchanged vector column" in withSessionFor(_.startsWith("5")) { cs =>
      val table = ks.table[VectorTableRow]
        .partition(Symbol("intColumn"))
        .cluster(Symbol("longColumn"))
        .build("vector_migrate_table")

      cs.create(ks).unsafeRunSync()
      cs.create(table).unsafeRunSync()

      cs.migrateDDL(table).unsafeRunSync() shouldBe Nil
    }

    "will not emit alter for SAI-indexed vector column" in withSessionFor(_.startsWith("5")) { cs =>
      val table = ks.table[VectorTableRow]
        .partition(Symbol("intColumn"))
        .indexBySAIVector(Symbol("vector8FloatColumn"), "vec_migrate_idx", SimilarityFunction.COSINE)
        .build("vector_migrate_idx_table")

      cs.create(ks).unsafeRunSync()
      cs.create(table).unsafeRunSync()

      cs.migrateDDL(table).unsafeRunSync() shouldBe Nil
    }

    "will recreate SAI index when options change" in withSessionFor(_.startsWith("5")) { cs =>
      val tableV1 = ks.table[FooTable1].partition(Symbol("intColumn"))
        .indexBySAI(Symbol("strColumn"), "str_sai_idx3", Map("case_sensitive" -> "true"))
        .build("foo_sai3")
      val tableV2 = ks.table[FooTable1].partition(Symbol("intColumn"))
        .indexBySAI(Symbol("strColumn"), "str_sai_idx3", Map("case_sensitive" -> "false"))
        .build("foo_sai3")

      cs.create(ks).unsafeRunSync()
      cs.create(tableV1).unsafeRunSync()

      val migrate = cs.migrateDDL(tableV2).unsafeRunSync()
      migrate.size shouldBe 2
      migrate.head shouldBe "DROP INDEX crud_ks.str_sai_idx3"
      migrate(1) should include("case_sensitive")

      migrate.foreach(cs.executeCql(_).unsafeRunSync())
      cs.migrateDDL(tableV2).unsafeRunSync() shouldBe Nil
    }

  }
}
