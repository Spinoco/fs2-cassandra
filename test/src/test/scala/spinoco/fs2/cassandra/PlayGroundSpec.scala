package spinoco.fs2.cassandra

import shapeless.tag
import shapeless.tag.@@
import spinoco.fs2.cassandra.CType.Ascii
import spinoco.fs2.cassandra.sample.SimpleTableRow
import spinoco.fs2.cassandra.support.CassandraDefinition


class PlayGroundSpec

 extends SchemaSupport
{

  override lazy val cassandra: CassandraDefinition = CassandraDefinition.`3.5`

  override lazy val clearContainers: Boolean = false
  override lazy val startContainers: Boolean = false

  //override def preserveKeySpace(s: String): Boolean =


  s"Playground (${cassandra.tag})" - {

    "will query by index" in  withSessionAndSimpleSchema { cs =>
      val update =
        simpleTable.update.set('asciiColumn)
        .build
        .fromHList.fromTuple[(String @@ Ascii,Int,Long)]

      cs.execute(update)((tag[Ascii]("ascii1"),1,1)).unsafeRun()

      val query =
        simpleTable.query.all
          .byIndex('asciiColumn, Comparison.EQ)
          .build
          .fromA[String @@ Ascii]
          .as[SimpleTableRow]

      val r = cs.query(query)(tag[Ascii]("ascii1")).runLog.unsafeRun

      r.size shouldBe 1
      r.map(_.asciiColumn) shouldBe Vector(tag[Ascii]("ascii1"))

//      val cs = sessionInstance.map(_._1).get
//      val cst = sessionInstance.map(_._2).get
//
//      val updateIfExists =
//        simpleTable.update
//          .set('stringColumn)
//          .onlyIfExists
//          .build
//          .fromHList
//          .fromTuple[(String,Int,Long)]
//          .asA
//
//
//      cs.execute(ks.cqlStatement)
//
//      cs.execute(simpleTable.cqlStatement)
//      cs.execute(strInsert.cqlFor(SimpleTableRow.simpleInstance))
//      cs.execute(strInsert.cqlFor(SimpleTableRow.simpleInstance.copy(longColumn = 20)))
//
//      println(ks.cqlStatement)
//      println(simpleTable.cqlStatement)
//      println(strInsert.cqlFor(SimpleTableRow.simpleInstance))
//      println(strInsert.cqlFor(SimpleTableRow.simpleInstance.copy(longColumn = 20)))
//
//
//     // cst.execute(strInsert)(SimpleTableRow.simpleInstance).unsafeRun
//     // cst.execute(strInsert)(SimpleTableRow.simpleInstance.copy(longColumn = 20)).unsafeRun
//
//      val r3 = cs.executeAsync(s"SELECT * from ${simpleTable.fullName}").get()
//      r3.all().asScala.foreach(println)
//
//      val ps = cs.prepareAsync(updateIfExists.cqlStatement).get()
//      val bs1 = ps.bind()
//      bs1.setInt("intColumn", 1)
//      bs1.setLong("longColumn", 2)
//      bs1.setString("stringColumn", "U1")
//
//      val bs2 = ps.bind()
//      bs2.setInt("intColumn", 1)
//      bs2.setLong("longColumn", 20)
//      bs2.setString("stringColumn", "U2")
//
//      val batch = new com.datastax.driver.core.BatchStatement()
//      batch.add(bs1)
//      batch.add(bs2)
//      val r = cs.executeAsync(batch).get()
//
//      println(s"APPLIED ${r.wasApplied()}")
//      r.all().asScala.foreach(println)
//      println("---------------------------")
//
//      val r2 = cs.executeAsync(s"SELECT * from ${simpleTable.fullName}").get()
//      r2.all().asScala.foreach(println)

    }
  }

}
