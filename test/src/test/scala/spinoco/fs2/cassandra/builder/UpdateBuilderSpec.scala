package spinoco.fs2.cassandra.builder

import shapeless.LabelledGeneric
import spinoco.fs2.cassandra.sample.{CounterTableRow, ListTableRow, MapTableRow, SimpleTableRow}
import spinoco.fs2.cassandra.support.Fs2CassandraSpec
import spinoco.fs2.cassandra.{Comparison, KeySpace}


class UpdateBuilderSpec extends Fs2CassandraSpec {

  val ks = new KeySpace("test_ks")

  "UPDATE statement" - {

    val simpleTable =
      ks.table[SimpleTableRow]
        .partition(Symbol("intColumn"))
        .cluster(Symbol("longColumn"))
        .build("test_table")

    val listTable =
      ks.table[ListTableRow]
        .partition(Symbol("intColumn"))
        .cluster(Symbol("longColumn"))
        .build("list_table")

    val mapTable =
      ks.table[MapTableRow]
        .partition(Symbol("intColumn"))
        .cluster(Symbol("longColumn"))
        .build("map_table")

    val counterTable =
      ks.table[CounterTableRow]
        .partition(Symbol("intColumn"))
        .cluster(Symbol("longColumn"))
        .build("counter_table")


    "will update all columns for PK" in {

      simpleTable
        .update.all
        .build.cqlStatement shouldBe
        "UPDATE test_ks.test_table SET stringColumn = :stringColumn,asciiColumn = :asciiColumn,floatColumn = :floatColumn,doubleColumn = :doubleColumn,bigDecimalColumn = :bigDecimalColumn,bigIntColumn = :bigIntColumn,blobColumn = :blobColumn,uuidColumn = :uuidColumn,timeUuidColumn = :timeUuidColumn,durationColumn = :durationColumn,inetAddressColumn = :inetAddressColumn,enumColumn = :enumColumn" +
          " WHERE intColumn = :intColumn AND longColumn = :longColumn"

    }


    "will update single row for PK" in {

      simpleTable
      .update
      .set(Symbol("stringColumn"))
      .build.cqlStatement shouldBe
      "UPDATE test_ks.test_table SET stringColumn = :stringColumn" +
        " WHERE intColumn = :intColumn AND longColumn = :longColumn"

    }


    "will update single row if exists" in {
      simpleTable
      .update
      .set(Symbol("stringColumn"))
      .onlyIfExists
      .build.cqlStatement shouldBe
      "UPDATE test_ks.test_table SET stringColumn = :stringColumn" +
        " WHERE intColumn = :intColumn AND longColumn = :longColumn" +
        " IF EXISTS"
    }


    "will update single row if condition" in {


      simpleTable
        .update
        .set(Symbol("stringColumn"))
        .onlyIf(Symbol("asciiColumn"), Comparison.GT)
        .build.cqlStatement shouldBe
        "UPDATE test_ks.test_table SET stringColumn = :stringColumn" +
          " WHERE intColumn = :intColumn AND longColumn = :longColumn" +
          " IF asciiColumn > :asciiColumn"
    }

    "will update single row if condition alias" in {
      simpleTable
        .update
        .set(Symbol("stringColumn"))
        .onlyIf(Symbol("asciiColumn"), "str_greater",Comparison.GT)
        .build.cqlStatement shouldBe
        "UPDATE test_ks.test_table SET stringColumn = :stringColumn" +
          " WHERE intColumn = :intColumn AND longColumn = :longColumn" +
          " IF asciiColumn > :str_greater"
    }



    "will update list column" in {
      listTable
        .update
        .set(Symbol("listColumn"))
        .build.cqlStatement shouldBe
        "UPDATE test_ks.list_table SET listColumn = :listColumn" +
          " WHERE intColumn = :intColumn AND longColumn = :longColumn"
    }

    "will append to list column" in {
      listTable
        .update
        .append(Symbol("listColumn"))
        .build.cqlStatement shouldBe
        "UPDATE test_ks.list_table SET listColumn = listColumn + :listColumn" +
          " WHERE intColumn = :intColumn AND longColumn = :longColumn"

    }

    "will prepend to list column" in {
      listTable
        .update
        .prepend(Symbol("listColumn"))

        .build.cqlStatement shouldBe
        "UPDATE test_ks.list_table SET listColumn = :listColumn + listColumn" +
          " WHERE intColumn = :intColumn AND longColumn = :longColumn"

    }

    "will set at index list column" in {
      listTable
        .update
        .addAt(Symbol("listColumn") ,1)
        .build.cqlStatement shouldBe
        "UPDATE test_ks.list_table SET listColumn[1] = :listColumn" +
          " WHERE intColumn = :intColumn AND longColumn = :longColumn"

    }


    "will remove from list column" in {
      listTable
        .update
        .remove(Symbol("listColumn"))
        .build.cqlStatement shouldBe
        "UPDATE test_ks.list_table SET listColumn = listColumn - :listColumn" +
          " WHERE intColumn = :intColumn AND longColumn = :longColumn"

    }

    "will update set column" in {
      listTable
        .update
        .set(Symbol("setColumn"))
        .build.cqlStatement shouldBe
        "UPDATE test_ks.list_table SET setColumn = :setColumn" +
          " WHERE intColumn = :intColumn AND longColumn = :longColumn"
    }

    "will add to set" in {
      listTable
        .update
        .add(Symbol("setColumn"))
        .build.cqlStatement shouldBe
         "UPDATE test_ks.list_table SET setColumn = setColumn + :setColumn" +
           " WHERE intColumn = :intColumn AND longColumn = :longColumn"

    }

    "will remove from set" in {
      listTable
        .update
        .remove(Symbol("setColumn"))
        .build.cqlStatement shouldBe
        "UPDATE test_ks.list_table SET setColumn = setColumn - :setColumn" +
          " WHERE intColumn = :intColumn AND longColumn = :longColumn"

    }


    "will update map column" in {
      mapTable
        .update
        .set(Symbol("mapStringColumn"))
        .build.cqlStatement shouldBe
        "UPDATE test_ks.map_table SET mapStringColumn = :mapStringColumn" +
          " WHERE intColumn = :intColumn AND longColumn = :longColumn"

    }


    "will append (k,v) pair(s) to map columns" in {
      mapTable
        .update
        .addToMap(Symbol("mapStringColumn"), "new_entry")
        .build.cqlStatement shouldBe
        "UPDATE test_ks.map_table SET mapStringColumn = mapStringColumn + :new_entry" +
          " WHERE intColumn = :intColumn AND longColumn = :longColumn"

    }

    "will remove (k,v) pair(s) from map columns" in {
      mapTable
        .update
        .removeFromMap(Symbol("mapStringColumn"), "remove_keys")
        .build.cqlStatement shouldBe
        "UPDATE test_ks.map_table SET mapStringColumn = mapStringColumn - :remove_keys" +
          " WHERE intColumn = :intColumn AND longColumn = :longColumn"

    }

    "will update with ttl" in {
      simpleTable
        .update
        .set(Symbol("stringColumn"))
        .withTTL(Symbol("ttl"))
        .build.cqlStatement shouldBe
      "UPDATE test_ks.test_table USING  TTL :ttl  SET stringColumn = :stringColumn" +
        " WHERE intColumn = :intColumn AND longColumn = :longColumn"

    }

    "will update with timestamp" in {
      simpleTable
        .update
        .set(Symbol("stringColumn"))
        .withTimeStamp(Symbol("ts"))
        .build.cqlStatement shouldBe
        "UPDATE test_ks.test_table USING TIMESTAMP :ts  SET stringColumn = :stringColumn" +
          " WHERE intColumn = :intColumn AND longColumn = :longColumn"

    }

    "will update with timestamp and ttl" in {
      simpleTable
        .update
        .set(Symbol("stringColumn"))
        .withTimeStamp(Symbol("ts"))
        .withTTL(Symbol("ttl"))
        .build.cqlStatement shouldBe
        "UPDATE test_ks.test_table USING  TTL :ttl AND TIMESTAMP :ts  SET stringColumn = :stringColumn" +
          " WHERE intColumn = :intColumn AND longColumn = :longColumn"

    }

    "will increment counter" in {
      counterTable.update
        .increment(Symbol("counterColumn"))
        .build.cqlStatement shouldBe
        "UPDATE test_ks.counter_table SET counterColumn = counterColumn + :counterColumn" +
          " WHERE intColumn = :intColumn AND longColumn = :longColumn"
    }

    "will decrement counter" in {
      counterTable.update
        .decrement(Symbol("counterColumn"))
        .build.cqlStatement shouldBe
        "UPDATE test_ks.counter_table SET counterColumn = counterColumn - :counterColumn" +
          " WHERE intColumn = :intColumn AND longColumn = :longColumn"
    }

    "will fill in cqlFor" in {
      counterTable.update
      .decrement(Symbol("counterColumn"))
      .build
      .fromHList.fromTuple[(Long, Int, Long)]
      .cqlFor((1L, 2, 3L)) shouldBe
        "UPDATE test_ks.counter_table SET counterColumn = counterColumn - 1" +
          " WHERE intColumn = 2 AND longColumn = 3"

    }

    "will update columns" in {

      val generic = LabelledGeneric[SimpleTableRow]

      simpleTable
      .update.setColumns[generic.Repr]
      .build.cqlStatement shouldBe
        "UPDATE test_ks.test_table SET stringColumn = :stringColumn,asciiColumn = :asciiColumn,floatColumn = :floatColumn,doubleColumn = :doubleColumn,bigDecimalColumn = :bigDecimalColumn,bigIntColumn = :bigIntColumn,blobColumn = :blobColumn,uuidColumn = :uuidColumn,timeUuidColumn = :timeUuidColumn,durationColumn = :durationColumn,inetAddressColumn = :inetAddressColumn,enumColumn = :enumColumn" +
          " WHERE intColumn = :intColumn AND longColumn = :longColumn"
    }

  }




}
