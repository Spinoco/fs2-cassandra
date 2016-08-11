package spinoco.fs2.cassandra.builder

import spinoco.fs2.cassandra.KeySpace
import spinoco.fs2.cassandra.sample.SimpleTableRow
import spinoco.fs2.cassandra.support.Fs2CassandraSpec

/**
  * Created by adamchlupacek on 10/08/16.
  */
class MaterializedViewBuilderSpec extends Fs2CassandraSpec{

  val ks = new KeySpace("test_ks")

  "Materialize view" - {

    val simpleTableCompoundPk =
      ks.table[SimpleTableRow]
      .partition('intColumn)
      .partition('longColumn)
      .cluster('stringColumn)
      .build("test_table")

    "Create view" in {

      simpleTableCompoundPk.query
      .column('doubleColumn)
      .column('floatColumn)
      .materialize
      .partition('doubleColumn)
      .cluster('intColumn)
      .cluster('longColumn)
      .cluster('stringColumn)
      .build("materialized")
      .cqlStatement shouldBe Seq(
        "CREATE MATERIALIZED VIEW test_ks.materialized AS " +
        "SELECT doubleColumn,floatColumn FROM test_ks.test_table " +
        "WHERE doubleColumn IS NOT NULL AND intColumn IS NOT NULL AND longColumn IS NOT NULL AND stringColumn IS NOT NULL " +
        "PRIMARY KEY ((doubleColumn),intColumn,longColumn,stringColumn)"
      )
    }
  }
}
