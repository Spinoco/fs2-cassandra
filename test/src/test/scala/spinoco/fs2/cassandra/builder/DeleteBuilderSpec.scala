package spinoco.fs2.cassandra.builder

import spinoco.fs2.cassandra.{Comparison, KeySpace}
import spinoco.fs2.cassandra.sample.{OptionalTableRow, SimpleTableRow}
import spinoco.fs2.cassandra.support.Fs2CassandraSpec

/**
  * Created by pach on 11/06/16.
  */
class DeleteBuilderSpec extends Fs2CassandraSpec {

  val ks = new KeySpace("test_ks")

  "DELETE statement" - {

    val simpleTable =
      ks.table[SimpleTableRow]
        .partition('intColumn)
        .cluster('longColumn)
        .build("test_table")

    val optionalTable =
      ks.table[OptionalTableRow]
        .partition('intColumn)
        .cluster('longColumn)
        .build("optional_table")

    "will delete whole row" in {
      simpleTable.delete
        .row
        .build.cqlStatement shouldBe
      "DELETE FROM test_ks.test_table" +
        "  WHERE intColumn = :intColumn "
    }

    "will delete cluster from the row" in {
      simpleTable.delete
        .row
        .cluster('longColumn)
        .build.cqlStatement shouldBe
      "DELETE FROM test_ks.test_table" +
        "  WHERE intColumn = :intColumn AND longColumn = :longColumn "
    }

    "will delete single column from the row" in {
      optionalTable.delete
        .column('stringColumn)
        .primary
        .build.cqlStatement shouldBe
      "DELETE stringColumn FROM test_ks.optional_table" +
        "  WHERE intColumn = :intColumn AND longColumn = :longColumn "

    }

    "will delete if exists" in {
      simpleTable.delete
        .row
        .onlyIfExists
        .build.cqlStatement shouldBe
      "DELETE FROM test_ks.test_table  " +
        "WHERE intColumn = :intColumn  IF EXISTS"

    }

    "will delete if cond" in {
      simpleTable.delete
        .row
        .primary
        .onlyIf('stringColumn, "as_string", Comparison.EQ)
        .build.cqlStatement shouldBe
        "DELETE FROM test_ks.test_table" +
          "  WHERE intColumn = :intColumn AND longColumn = :longColumn" +
          " IF stringColumn = :as_string"

    }



  }

}
