package spinoco.fs2.cassandra


trait MigrationsSpec  extends SchemaSupport {

  case class FooTable1(intColumn:Int, longColumn:Long, strColumn:String )
  case class FooTable2(intColumn:Int, longColumn1:Long, strColumn:Int )

  val table1 = ks.table[FooTable1].partition('intColumn).build("foo1")
  val table2 = ks.table[FooTable2].partition('intColumn).build("foo1")
  val table3 = ks.table[FooTable1].partition('intColumn).cluster('longColumn).build("foo1")
  val view1 = table1.query.column('strColumn).materialize.partition('intColumn).cluster('longColumn).build("bar1")
  val view2 = table1.query.column('strColumn).column('longColumn).materialize.partition('intColumn).cluster('longColumn).build("bar1")
  val view3 = table3.query.column('strColumn).materialize.partition('longColumn).cluster('intColumn).build("bar1")
  val view4 = table1.query.column('strColumn).materialize.partition('longColumn).cluster('intColumn).build("bar1")


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


    "will create materialized view if base table dropped" in withSession { cs =>
      if(cassandra.isV3Compatible){
        cs.create(ks).unsafeRun
        cs.create(table1).unsafeRun
        cs.create(view1).unsafeRun

        val migrate = cs.migrateDDL(view3).unsafeRun

        migrate shouldBe Seq(
          """CREATE MATERIALIZED VIEW crud_ks.bar1 AS SELECT strColumn FROM crud_ks.foo1 """
            + """WHERE longColumn IS NOT NULL AND intColumn IS NOT NULL PRIMARY KEY ((longColumn),intColumn)""".stripMargin
        )
      }
    }

    "will drop and create materialized view if select changed" in withSession { cs =>
      if(cassandra.isV3Compatible) {
        cs.create(ks).unsafeRun
        cs.create(table1).unsafeRun
        cs.create(view1).unsafeRun

        val migrate = cs.migrateDDL(view2).unsafeRun

        migrate shouldBe Seq(
          "DROP MATERIALIZED VIEW crud_ks.bar1"
          , "CREATE MATERIALIZED VIEW crud_ks.bar1 AS SELECT strColumn,longColumn FROM crud_ks.foo1 WHERE intColumn IS NOT NULL AND longColumn IS NOT NULL PRIMARY KEY ((intColumn),longColumn)"
        )
      }
    }

    "will drop and create materialized view if primary key changed" in withSession { cs =>
      if(cassandra.isV3Compatible) {
        cs.create(ks).unsafeRun
        cs.create(table1).unsafeRun
        cs.create(view1).unsafeRun

        val migrate = cs.migrateDDL(view4).unsafeRun

        migrate shouldBe Seq(
          "DROP MATERIALIZED VIEW crud_ks.bar1"
          , "CREATE MATERIALIZED VIEW crud_ks.bar1 AS SELECT strColumn FROM crud_ks.foo1 WHERE longColumn IS NOT NULL AND intColumn IS NOT NULL PRIMARY KEY ((longColumn),intColumn)"
        )
      }
    }

    "will not emit anything in case nothing changes" in withSession { cs =>
      if(cassandra.isV3Compatible) {
        cs.create(ks).unsafeRun
        cs.create(table1).unsafeRun
        cs.create(view1).unsafeRun

        val migrate = cs.migrateDDL(view1).unsafeRun

        migrate shouldBe Nil
      }
    }
  }
}
