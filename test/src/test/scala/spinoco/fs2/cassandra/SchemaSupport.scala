package spinoco.fs2.cassandra

import cats.effect.IO
import fs2.Stream._
import shapeless.LabelledGeneric
import spinoco.fs2.cassandra.sample.{ListTableRow, MapTableRow, OptionalTableRow, SimpleTableRow}
import spinoco.fs2.cassandra.support.{DockerCassandra, Fs2CassandraSpec}


trait SchemaSupport extends Fs2CassandraSpec with DockerCassandra {

  val ks = KeySpace("crud_ks")

  val simpleTable =
    ks.table[SimpleTableRow]
      .partition('intColumn)
      .cluster('longColumn)
      .indexBy('asciiColumn, "asciiColumn_idx")
      .build("test_table")

  val strInsert =
    simpleTable.insert
      .all
      .build
      .from[SimpleTableRow]

  val strSelectOne =
    simpleTable.query.all
      .partition
      .cluster('longColumn, Comparison.EQ)
      .build
      .fromHList
      .fromTuple[(Int,Long)]
      .as[SimpleTableRow]

  val strSelectAll =
    simpleTable.query.all
      .build
      .as[SimpleTableRow]


  val strGen = LabelledGeneric[SimpleTableRow]



  def createValuesAndSchema[A](cs:CassandraSession[IO])(table:Table[_,_,_,_], insert:Insert[A,_])(f: (Int,Long) => A):Unit = {
    val records =
      for {
        i <- 0 to 10
        l <- 0l to 10l
      } yield f(i,l)

    (for {
      _ <- cs.create(ks)
      _ <- cs.create(table)
      _ <- emits(records).flatMap(a => eval_(cs.execute(insert)(a))).compile.drain
    } yield ()).unsafeRunSync()

  }


  def withSessionAndSimpleSchema(f: CassandraSession[IO] => Any): Unit = {
    withSession { cs =>
      createValuesAndSchema(cs)(simpleTable,strInsert){ case (i,l) => SimpleTableRow.simpleInstance.copy(intColumn = i, longColumn = l)}
      f(cs)
    }
  }

  def withSessionAndEmptySimpleSchema(f: CassandraSession[IO] => Any): Unit = {
    withSession { cs =>
      (for {
        _ <- cs.create(ks)
        _ <- cs.create(simpleTable)
      } yield ()).unsafeRunSync()
      f(cs)
    }
  }


  val listTable =
    ks.table[ListTableRow]
      .partition('intColumn)
      .cluster('longColumn)
      .build("list_table")

  val ltInsert =
    listTable.insert
      .all
      .build
      .from[ListTableRow]

  val ltSelectOne =
    listTable.query
      .all
      .primary
      .build
      .fromHList
      .fromTuple[(Int,Long)]
      .as[ListTableRow]



  def withSessionAndListSchema(f: CassandraSession[IO] => Any): Unit = {
    withSession { cs =>
      createValuesAndSchema(cs)(listTable,ltInsert){ case (i,l) => ListTableRow.instance.copy(intColumn = i, longColumn = l)}
      f(cs)
    }
  }


  val mapTable =
    ks.table[MapTableRow]
      .partition('intColumn)
      .cluster('longColumn)
      .build("list_table")

  val mtInsert =
    mapTable.insert
      .all
      .build
      .from[MapTableRow]

  val mtSelectOne =
    mapTable.query
      .all
      .primary
      .build
      .fromHList
      .fromTuple[(Int,Long)]
      .as[MapTableRow]

  def withSessionAndMapSchema(f: CassandraSession[IO] => Any): Unit = {
    withSession { cs =>
      createValuesAndSchema(cs)(mapTable,mtInsert){ case (i,l) => MapTableRow.instance.copy(intColumn = i, longColumn = l)}
      f(cs)
    }
  }

  val optionalTable =
    ks.table[OptionalTableRow]
      .partition('intColumn)
      .cluster('longColumn)
      .build("optional_table")

  val otInsert =
    optionalTable.insert
      .all
      .build
      .from[OptionalTableRow]

  val otSelectAll =
    optionalTable.query.all
      .build
      .as[OptionalTableRow]

  def withSessionAndOptionalSchema(f: CassandraSession[IO] => Any): Unit = {
    withSession { cs =>
      createValuesAndSchema(cs)(optionalTable,otInsert){ case (i,l) => OptionalTableRow.instance.copy(intColumn = i, longColumn = l)}
      f(cs)
    }
  }

}
