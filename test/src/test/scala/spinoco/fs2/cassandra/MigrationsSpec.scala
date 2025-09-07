package spinoco.fs2.cassandra

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

  }
}
