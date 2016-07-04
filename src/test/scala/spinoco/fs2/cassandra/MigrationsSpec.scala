package spinoco.fs2.cassandra


trait MigrationsSpec  extends SchemaSupport {

  case class FooTable1(intColumn:Int, longColumn:Long, strColumn:String )
  case class FooTable2(intColumn:Int, longColumn1:Long, strColumn:Int )

  val table1 = ks.table[FooTable1].partition('intColumn).build("foo1")
  val table2 = ks.table[FooTable2].partition('intColumn).build("foo1")
  val table3 = ks.table[FooTable1].partition('intColumn).cluster('longColumn).build("foo1")


  s"Migrations (${cassandra.tag})" - {

    "will emit create for KeySpace that does not exists" in withSession { cs =>

      cs.migrateDDL(ks).unsafeRun shouldBe Seq(
        "CREATE KEYSPACE crud_ks WITH REPLICATION = {'class':'org.apache.cassandra.locator.SimpleStrategy','replication_factor':'1'} AND DURABLE_WRITES = true "
        )

    }

    "will not emit create//alter if KeySpace is same" in withSession { cs =>

      cs.create(ks).unsafeRun

      cs.migrateDDL(ks).unsafeRun shouldBe Nil

    }

    "will update KeySpace DURABLE_WRITES" in withSession { cs =>

      cs.create(ks).unsafeRun

      val migrate = cs.migrateDDL(ks.withDurableWrites(durable = false)).unsafeRun

      migrate shouldBe Seq("ALTER KEYSPACE crud_ks WITH DURABLE_WRITES = false")

      migrate.foreach(cs.executeCql(_).unsafeRun)

      cs.migrateDDL(ks.withDurableWrites(durable = false)).unsafeRun shouldBe Nil

    }

    "will update KeySpace REPLICATION" in withSession { cs =>

      cs.create(ks).unsafeRun

      val migrate = cs.migrateDDL(ks.copy(strategyOptions = Seq("replication_factor" -> "2"))).unsafeRun

      migrate shouldBe Seq("ALTER KEYSPACE crud_ks WITH REPLICATION = {'replication_factor':'2','class':'org.apache.cassandra.locator.SimpleStrategy'}")

      migrate.foreach(cs.executeCql(_).unsafeRun)

      cs.migrateDDL(ks.copy(strategyOptions = Seq("replication_factor" -> "2"))).unsafeRun shouldBe Nil

    }


    "will emit create table if table does not exists" in withSession { cs =>
      cs.create(ks).unsafeRun

      val migrate = cs.migrateDDL(table1).unsafeRun

      migrate shouldBe Seq(
        "CREATE TABLE crud_ks.foo1 (intColumn int,longColumn bigint,strColumn varchar, PRIMARY KEY ((intColumn)))"
      )
    }

    "will not emit update if schema is same" in withSession { cs =>
      cs.create(ks).unsafeRun
      cs.create(table1).unsafeRun

      val migrate = cs.migrateDDL(table1).unsafeRun

      migrate shouldBe Nil

    }


    "will update column defs if they have changed" in withSession { cs =>
      cs.create(ks).unsafeRun
      cs.create(table1).unsafeRun

      val migrate = cs.migrateDDL(table2).unsafeRun

      migrate shouldBe Seq(
        "ALTER TABLE crud_ks.foo1 DROP longcolumn"
        ,"ALTER TABLE crud_ks.foo1 DROP strcolumn"
        , "ALTER TABLE crud_ks.foo1 ADD longColumn1 bigint"
        , "ALTER TABLE crud_ks.foo1 ADD strColumn int"
      )

    }


    "will drop and create table if primary key has changed" in withSession { cs =>
      cs.create(ks).unsafeRun
      cs.create(table1).unsafeRun

      val migrate = cs.migrateDDL(table3).unsafeRun

      migrate shouldBe Seq(
        "DROP TABLE crud_ks.foo1"
        , "CREATE TABLE crud_ks.foo1 (intColumn int,longColumn bigint,strColumn varchar, PRIMARY KEY ((intColumn),longColumn))"
      )
    }


  }

}
