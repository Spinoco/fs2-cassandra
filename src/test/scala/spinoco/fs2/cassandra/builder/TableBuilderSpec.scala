package spinoco.fs2.cassandra.builder


import spinoco.fs2.cassandra.KeySpace
import spinoco.fs2.cassandra.sample._
import spinoco.fs2.cassandra.support.Fs2CassandraSpec



class TableBuilderSpec extends Fs2CassandraSpec{



  val ks = new KeySpace("test_ks")


  "DDL for table for simple types with" - {


    val simpleTableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,stringColumn varchar,asciiColumn ascii,floatColumn float,doubleColumn double,bigDecimalColumn decimal,bigIntColumn varint,blobColumn blob,uuidColumn uuid,timeUuidColumn timeuuid,durationColumn bigint,inetAddressColumn inet,enumColumn varchar,"

    "partition key" in {
     val table =
       ks.table[SimpleTableRow]
         .partition('intColumn)
         .createTable("test_table")

     table.cqlStatement shouldBe s"$simpleTableDef PRIMARY KEY ((intColumn)))"
    }

    "cluster key" in {
      val table =
        ks.table[SimpleTableRow]
        .partition('intColumn)
        .cluster('longColumn)
        .createTable("test_table")

      table.cqlStatement shouldBe s"$simpleTableDef PRIMARY KEY ((intColumn),longColumn))"
    }

    "compound partition key" in {
      val table =
        ks.table[SimpleTableRow]
          .partition('intColumn)
          .partition('longColumn)
          .createTable("test_table")

      table.cqlStatement shouldBe s"$simpleTableDef PRIMARY KEY ((intColumn,longColumn)))"
    }

    "compound cluster key" in {
      val table =
        ks.table[SimpleTableRow]
          .partition('intColumn)
          .cluster('longColumn)
          .cluster('stringColumn)
          .createTable("test_table")

      table.cqlStatement shouldBe s"$simpleTableDef PRIMARY KEY ((intColumn),longColumn,stringColumn))"
    }

  }


  "DDL for table with options with " - {


    val tableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,stringColumn varchar,asciiColumn ascii,enumColumn varchar,listColumn list<varchar>,setColumn set<varchar>,vectorColumn list<varchar>,"

    "partition key" in {
      val table =
        ks.table[OptionalTableRow]
          .partition('intColumn)
          .createTable("test_table")


      table.cqlStatement shouldBe s"$tableDef PRIMARY KEY ((intColumn)))"
    }


  }


  "DDL for table with List/Seq/Set/Vector with " - {


    val tableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,listColumn list<varchar>,setColumn set<varchar>,vectorColumn list<varchar>,seqColumn list<varchar>,"


    "partition key" in {
      val table =
        ks.table[ListTableRow]
          .partition('intColumn)
          .createTable("test_table")



      table.cqlStatement shouldBe  s"$tableDef PRIMARY KEY ((intColumn)))"
    }

  }



  "DDL for table with tuples with " - {


    val tableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,tuple2Column frozen<tuple<varchar, int>>,tuple3Column frozen<tuple<varchar, ascii, bigint>>,tuple4Column frozen<tuple<varchar, ascii, uuid, timeuuid>>,tuple5Column frozen<tuple<varchar, ascii, uuid, timeuuid, timestamp>>,"


    "partition key" in {
      val table =
        ks.table[TupleTableRow]
          .partition('intColumn)
          .createTable("test_table")


      table.cqlStatement shouldBe  s"$tableDef PRIMARY KEY ((intColumn)))"
    }


  }


  "DDL for table with maps with " - {


    val tableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,mapStringColumn map<varchar, varchar>,mapIntColumn map<int, varchar>,"

    "partition key" in {
      val table =
        ks.table[MapTableRow]
          .partition('intColumn)
          .createTable("test_table")



      table.cqlStatement shouldBe s"$tableDef PRIMARY KEY ((intColumn)))"
    }

  }


}
