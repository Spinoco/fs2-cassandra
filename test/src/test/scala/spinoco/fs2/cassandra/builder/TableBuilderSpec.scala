package spinoco.fs2.cassandra.builder


import shapeless.LabelledGeneric
import spinoco.fs2.cassandra.KeySpace
import spinoco.fs2.cassandra.sample._
import spinoco.fs2.cassandra.support.Fs2CassandraSpec



class TableBuilderSpec extends Fs2CassandraSpec{



  val ks = new KeySpace("test_ks")


  "DDL for table for simple types with" - {


    val simpleTableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,stringColumn text,asciiColumn ascii,floatColumn float,doubleColumn double,bigDecimalColumn decimal,bigIntColumn varint,blobColumn blob,uuidColumn uuid,timeUuidColumn timeuuid,durationColumn bigint,inetAddressColumn inet,enumColumn text,"

    "partition key" in {
     val table =
       ks.table[SimpleTableRow]
         .partition('intColumn)
         .build("test_table")

     table.cqlStatement shouldBe Seq(s"$simpleTableDef PRIMARY KEY ((intColumn)))")
    }

    "cluster key" in {
      val table =
        ks.table[SimpleTableRow]
        .partition('intColumn)
        .cluster('longColumn)
        .build("test_table")

      table.cqlStatement shouldBe Seq(s"$simpleTableDef PRIMARY KEY ((intColumn),longColumn))")
    }

    "compound partition key" in {
      val table =
        ks.table[SimpleTableRow]
          .partition('intColumn)
          .partition('longColumn)
          .build("test_table")

      table.cqlStatement shouldBe Seq(s"$simpleTableDef PRIMARY KEY ((intColumn,longColumn)))")
    }

    "compound cluster key" in {
      val table =
        ks.table[SimpleTableRow]
          .partition('intColumn)
          .cluster('longColumn)
          .cluster('stringColumn)
          .build("test_table")

      table.cqlStatement shouldBe Seq(s"$simpleTableDef PRIMARY KEY ((intColumn),longColumn,stringColumn))")
    }

    "indexed" in {
      val table =
        ks.table[SimpleTableRow]
          .partition('intColumn)
          .indexBy('asciiColumn, "asciiColumn_idx")
          .indexBy('enumColumn, "enumColumn_idx")
          .build("test_table")

      table.cqlStatement.toSet shouldBe Set(
        s"$simpleTableDef PRIMARY KEY ((intColumn)))"
        , "CREATE INDEX enumColumn_idx ON test_ks.test_table (enumColumn)"
        , "CREATE INDEX asciiColumn_idx ON test_ks.test_table (asciiColumn)"
      )
    }

    "sasi indexes" in {
      val table =
        ks.table[SimpleTableRow]
          .partition('intColumn)
          .indexByPrefix('asciiColumn, "prefix_index")
          .indexBySparse('floatColumn, "sparse_index")
          .indexByContains('doubleColumn, "contains_index")
          .build("test_table")

      table.cqlStatement.toSet shouldBe Set(
        s"$simpleTableDef PRIMARY KEY ((intColumn)))"
        , "CREATE CUSTOM INDEX contains_index ON test_ks.test_table (doubleColumn) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'CONTAINS'}"
        , "CREATE CUSTOM INDEX sparse_index ON test_ks.test_table (floatColumn) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'SPARSE'}"
        , "CREATE CUSTOM INDEX prefix_index ON test_ks.test_table (asciiColumn) USING 'org.apache.cassandra.index.sasi.SASIIndex'"
      )

    }

    "add colums" in {

      case class DummyClass(
        name: String
        , height: String
      )

      val generic = LabelledGeneric[DummyClass]

      val tableDef = "CREATE TABLE test_ks.test_table (name text,height text,intColumn int,longColumn bigint,stringColumn text,asciiColumn ascii,floatColumn float,doubleColumn double,bigDecimalColumn decimal,bigIntColumn varint,blobColumn blob,uuidColumn uuid,timeUuidColumn timeuuid,durationColumn bigint,inetAddressColumn inet,enumColumn text, PRIMARY KEY ((intColumn)))"

      ks.table[SimpleTableRow]
      .partition('intColumn)
      .columns[generic.Repr]
      .build("test_table").cqlStatement shouldBe Seq(tableDef)
    }

  }


  "DDL for table with options with " - {


    val tableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,stringColumn text,asciiColumn ascii,enumColumn text,listColumn list<text>,setColumn set<text>,vectorColumn list<text>,"

    "partition key" in {
      val table =
        ks.table[OptionalTableRow]
          .partition('intColumn)
          .build("test_table")


      table.cqlStatement shouldBe Seq(s"$tableDef PRIMARY KEY ((intColumn)))")
    }


  }


  "DDL for table with List/Seq/Set/Vector with " - {


    val tableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,listColumn list<text>,setColumn set<text>,vectorColumn list<text>,seqColumn list<text>,"


    "partition key" in {
      val table =
        ks.table[ListTableRow]
          .partition('intColumn)
          .build("test_table")



      table.cqlStatement shouldBe  Seq(s"$tableDef PRIMARY KEY ((intColumn)))")
    }

  }



  "DDL for table with tuples with " - {


    val tableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,tuple2Column frozen<tuple<text, int>>,tuple3Column frozen<tuple<text, ascii, bigint>>,tuple4Column frozen<tuple<text, ascii, uuid, timeuuid>>,tuple5Column frozen<tuple<text, ascii, uuid, timeuuid, timestamp>>,"


    "partition key" in {
      val table =
        ks.table[TupleTableRow]
          .partition('intColumn)
          .build("test_table")


      table.cqlStatement shouldBe  Seq(s"$tableDef PRIMARY KEY ((intColumn)))")
    }


  }


  "DDL for table with maps with " - {


    val tableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,mapStringColumn map<text, text>,mapIntColumn map<int, text>,"

    "partition key" in {
      val table =
        ks.table[MapTableRow]
          .partition('intColumn)
          .build("test_table")



      table.cqlStatement shouldBe Seq(s"$tableDef PRIMARY KEY ((intColumn)))")
    }

  }



}
