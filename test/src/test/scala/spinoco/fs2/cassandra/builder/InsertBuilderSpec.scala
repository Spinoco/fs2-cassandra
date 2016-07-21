package spinoco.fs2.cassandra.builder

import spinoco.fs2.cassandra.KeySpace
import spinoco.fs2.cassandra.sample.SimpleTableRow
import spinoco.fs2.cassandra.support.Fs2CassandraSpec


class InsertBuilderSpec extends Fs2CassandraSpec {

  val ks = new KeySpace("test_ks")

  "INSERT for table with simple types" - {

    val table =
      ks.table[SimpleTableRow]
        .partition('intColumn)
        .cluster('longColumn)
        .build("test_table")

    "with simple primary key all columns" in {

      table.insert.all.build.cqlStatement shouldBe
        "INSERT INTO test_ks.test_table (intColumn,longColumn,stringColumn,asciiColumn,floatColumn,doubleColumn,bigDecimalColumn,bigIntColumn,blobColumn,uuidColumn,timeUuidColumn,durationColumn,inetAddressColumn,enumColumn)" +
          " VALUES (:intColumn,:longColumn,:stringColumn,:asciiColumn,:floatColumn,:doubleColumn,:bigDecimalColumn,:bigIntColumn,:blobColumn,:uuidColumn,:timeUuidColumn,:durationColumn,:inetAddressColumn,:enumColumn)   "

    }

    "with ifNotExists flag" in {
      table.insert.all.ifNotExists.build.cqlStatement shouldBe
      "INSERT INTO test_ks.test_table (intColumn,longColumn,stringColumn,asciiColumn,floatColumn,doubleColumn,bigDecimalColumn,bigIntColumn,blobColumn,uuidColumn,timeUuidColumn,durationColumn,inetAddressColumn,enumColumn)" +
        " VALUES (:intColumn,:longColumn,:stringColumn,:asciiColumn,:floatColumn,:doubleColumn,:bigDecimalColumn,:bigIntColumn,:blobColumn,:uuidColumn,:timeUuidColumn,:durationColumn,:inetAddressColumn,:enumColumn)" +
        " IF NOT EXISTS  "
    }


    "with ttl specified" in {
      table.insert.all.withTTL("ttl").build.cqlStatement shouldBe
      "INSERT INTO test_ks.test_table (intColumn,longColumn,stringColumn,asciiColumn,floatColumn,doubleColumn,bigDecimalColumn,bigIntColumn,blobColumn,uuidColumn,timeUuidColumn,durationColumn,inetAddressColumn,enumColumn)" +
        " VALUES (:intColumn,:longColumn,:stringColumn,:asciiColumn,:floatColumn,:doubleColumn,:bigDecimalColumn,:bigIntColumn,:blobColumn,:uuidColumn,:timeUuidColumn,:durationColumn,:inetAddressColumn,:enumColumn)" +
        "  USING TTL :ttl "
    }

    "with timestamp specified" in {
      table.insert.all.withTimestamp("timestamp").build.cqlStatement shouldBe
      "INSERT INTO test_ks.test_table (intColumn,longColumn,stringColumn,asciiColumn,floatColumn,doubleColumn,bigDecimalColumn,bigIntColumn,blobColumn,uuidColumn,timeUuidColumn,durationColumn,inetAddressColumn,enumColumn)" +
        " VALUES (:intColumn,:longColumn,:stringColumn,:asciiColumn,:floatColumn,:doubleColumn,:bigDecimalColumn,:bigIntColumn,:blobColumn,:uuidColumn,:timeUuidColumn,:durationColumn,:inetAddressColumn,:enumColumn)" +
        "  USING TIMESTAMP :timestamp "
    }

    "with ttl, timestamp" in {
      table.insert.all
        .withTimestamp("timestamp")
        .withTTL("ttl")
        .build.cqlStatement shouldBe
      "INSERT INTO test_ks.test_table (intColumn,longColumn,stringColumn,asciiColumn,floatColumn,doubleColumn,bigDecimalColumn,bigIntColumn,blobColumn,uuidColumn,timeUuidColumn,durationColumn,inetAddressColumn,enumColumn)" +
        " VALUES (:intColumn,:longColumn,:stringColumn,:asciiColumn,:floatColumn,:doubleColumn,:bigDecimalColumn,:bigIntColumn,:blobColumn,:uuidColumn,:timeUuidColumn,:durationColumn,:inetAddressColumn,:enumColumn)" +
        "  USING TTL :ttl AND TIMESTAMP :timestamp "
    }


    "with only subset of columns specified" in {
        table.insert.column('stringColumn).build.cqlStatement shouldBe
      "INSERT INTO test_ks.test_table (stringColumn,intColumn,longColumn) VALUES (:stringColumn,:intColumn,:longColumn)   "
    }

    "will fill cqlFor" in {
      table.insert.column('stringColumn).build.fromHList.fromTuple[(String, Int, Long)].cqlFor(("Hello", 1 ,2)) shouldBe
        "INSERT INTO test_ks.test_table (stringColumn,intColumn,longColumn) VALUES ('Hello',1,2)   "
    }

  }

}
