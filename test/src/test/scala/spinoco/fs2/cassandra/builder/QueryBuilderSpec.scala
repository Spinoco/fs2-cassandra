package spinoco.fs2.cassandra.builder

import spinoco.fs2.cassandra.{Comparison, KeySpace, functions}
import spinoco.fs2.cassandra.sample.SimpleTableRow
import spinoco.fs2.cassandra.support.Fs2CassandraSpec


class QueryBuilderSpec extends Fs2CassandraSpec {

  val ks = new KeySpace("test_ks")

  "SELECT statement" - {

    val simpleTable =
      ks.table[SimpleTableRow]
        .partition('intColumn)
        .cluster('longColumn)
        .build("test_table")

    val simpleTableCompoundPk =
      ks.table[SimpleTableRow]
        .partition('intColumn)
        .partition('longColumn)
        .cluster('stringColumn)
        .build("test_table")

    val simpleTableCompoundCk =
      ks.table[SimpleTableRow]
        .partition('intColumn)
        .cluster('longColumn)
        .cluster('stringColumn)
        .build("test_table")


    "will select all columns from given row" in {

      simpleTable.query
      .all
      .build
      .cqlStatement shouldBe
        "SELECT intColumn,longColumn,stringColumn,asciiColumn,floatColumn,doubleColumn,bigDecimalColumn,bigIntColumn,blobColumn,uuidColumn,timeUuidColumn,durationColumn,inetAddressColumn,enumColumn" +
        " FROM test_ks.test_table"

    }

    "will select single column from given row" in {
      simpleTable.query
      .column('stringColumn)
      .build
      .cqlStatement shouldBe
        "SELECT stringColumn" +
        " FROM test_ks.test_table"

    }

    "will select single column with alias from given row" in {
      simpleTable.query
        .columnAs('stringColumn, "as_alias")
        .build
        .cqlStatement shouldBe
        "SELECT stringColumn AS as_alias" +
          " FROM test_ks.test_table"

    }

    "will select count" in {
      simpleTable.query
        .function(functions.count, "count_of_rows")
        .build
        .cqlStatement shouldBe
        "SELECT count(*) AS count_of_rows" +
          " FROM test_ks.test_table"

    }

    "will select date of" in {
      simpleTable.query
        .functionAt(functions.dateOf, 'timeUuidColumn, "time_of_uuid")
        .build
        .cqlStatement shouldBe
        "SELECT dateOf(timeUuidColumn) AS time_of_uuid" +
          " FROM test_ks.test_table"

    }

    "will select timestamp of" in {
      simpleTable.query
        .functionAt(functions.unixTimestampOf, 'timeUuidColumn, "timestamp_of_uuid")
        .build
        .cqlStatement shouldBe
        "SELECT unixTimestampOf(timeUuidColumn) AS timestamp_of_uuid" +
          " FROM test_ks.test_table"

    }

    "will select write time of" in {
      simpleTable.query
        .functionAt(functions.writeTimeOfMicro[String], 'stringColumn, "write_time_of")
        .build
        .cqlStatement shouldBe
        "SELECT WRITETIME(stringColumn) AS write_time_of" +
          " FROM test_ks.test_table"

    }


    "will select ttl of" in {
      simpleTable.query
        .functionAt(functions.ttlOf[String], 'stringColumn, "ttl_of")
        .build
        .cqlStatement shouldBe
        "SELECT TTL(stringColumn) AS ttl_of" +
          " FROM test_ks.test_table"

    }


    "will select with partitioning key" in {

      simpleTable.query
      .column('stringColumn)
      .partition
      .build
      .cqlStatement shouldBe
      "SELECT stringColumn FROM test_ks.test_table" +
        " WHERE intColumn = :intColumn"

    }

    "will select with partitioning key for compound key" in {

      simpleTableCompoundPk.query
        .column('stringColumn)
        .partition
        .build
        .cqlStatement shouldBe
        "SELECT stringColumn FROM test_ks.test_table" +
          " WHERE intColumn = :intColumn AND longColumn = :longColumn"

    }


    "will select with partitioning key and cluster key" in {

      simpleTable.query
        .column('stringColumn)
        .partition
        .cluster('longColumn, Comparison.GTEQ)
        .build
        .cqlStatement shouldBe
        "SELECT stringColumn FROM test_ks.test_table" +
          " WHERE intColumn = :intColumn AND longColumn >= :longColumn"

    }

    "will select with partitioning key and cluster key (aliased)" in {
      simpleTable.query
        .column('stringColumn)
        .partition
        .cluster('longColumn, "greater", Comparison.GTEQ)
        .build
        .cqlStatement shouldBe
        "SELECT stringColumn FROM test_ks.test_table" +
          " WHERE intColumn = :intColumn AND longColumn >= :greater"

    }

    "will select with partitioning key and compound cluster key" in {
      simpleTableCompoundCk.query
        .column('stringColumn)
        .partition
        .cluster('longColumn, "greaterLong", Comparison.GTEQ)
        .cluster('stringColumn, "greaterString", Comparison.GTEQ)
        .build
        .cqlStatement shouldBe
        "SELECT stringColumn FROM test_ks.test_table" +
          " WHERE intColumn = :intColumn AND (longColumn,stringColumn) >= (:greaterLong,:greaterString)"

    }


    "will select with limit" in {
      simpleTable.query
      .column('stringColumn)
      .limit(1)
      .build
      .cqlStatement shouldBe
      "SELECT stringColumn" +
        " FROM test_ks.test_table LIMIT 1"
    }


    "will select with allow filtering" in {
      simpleTable.query
        .column('stringColumn)
        .allowFiltering
        .build
        .cqlStatement shouldBe
        "SELECT stringColumn" +
          " FROM test_ks.test_table WITH ALLOW FILTERING"
    }


    "will select with order by asc" in {
      simpleTable.query
        .column('stringColumn)
        .orderBy('longColumn, ascending = true)
        .build
        .cqlStatement shouldBe
        "SELECT stringColumn" +
          " FROM test_ks.test_table ORDER BY longColumn ASC"
    }


    "will select with order by desc" in {
      simpleTable.query
        .column('stringColumn)
        .orderBy('longColumn, ascending = false)
        .build
        .cqlStatement shouldBe
        "SELECT stringColumn" +
          " FROM test_ks.test_table ORDER BY longColumn DESC"
    }


  }


}
