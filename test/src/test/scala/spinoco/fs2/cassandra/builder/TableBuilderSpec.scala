package spinoco.fs2.cassandra.builder


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
         .partition(Symbol("intColumn"))
         .build("test_table")

     table.cqlStatement shouldBe Seq(s"$simpleTableDef PRIMARY KEY ((intColumn)))")
    }

    "cluster key" in {
      val table =
        ks.table[SimpleTableRow]
        .partition(Symbol("intColumn"))
        .cluster(Symbol("longColumn"))
        .build("test_table")

      table.cqlStatement shouldBe Seq(s"$simpleTableDef PRIMARY KEY ((intColumn),longColumn))")
    }

    "compound partition key" in {
      val table =
        ks.table[SimpleTableRow]
          .partition(Symbol("intColumn"))
          .partition(Symbol("longColumn"))
          .build("test_table")

      table.cqlStatement shouldBe Seq(s"$simpleTableDef PRIMARY KEY ((intColumn,longColumn)))")
    }

    "compound cluster key" in {
      val table =
        ks.table[SimpleTableRow]
          .partition(Symbol("intColumn"))
          .cluster(Symbol("longColumn"))
          .cluster(Symbol("stringColumn"))
          .build("test_table")

      table.cqlStatement shouldBe Seq(s"$simpleTableDef PRIMARY KEY ((intColumn),longColumn,stringColumn))")
    }

    "indexed" in {
      val table =
        ks.table[SimpleTableRow]
          .partition(Symbol("intColumn"))
          .indexBy(Symbol("asciiColumn"), "asciiColumn_idx")
          .indexBy(Symbol("enumColumn"), "enumColumn_idx")
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
          .partition(Symbol("intColumn"))
          .indexByPrefix(Symbol("asciiColumn"), "prefix_index")
          .indexBySparse(Symbol("floatColumn"), "sparse_index")
          .indexByContains(Symbol("doubleColumn"), "contains_index")
          .build("test_table")

      table.cqlStatement.toSet shouldBe Set(
        s"$simpleTableDef PRIMARY KEY ((intColumn)))"
        , "CREATE CUSTOM INDEX contains_index ON test_ks.test_table (doubleColumn) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'CONTAINS'}"
        , "CREATE CUSTOM INDEX sparse_index ON test_ks.test_table (floatColumn) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'SPARSE'}"
        , "CREATE CUSTOM INDEX prefix_index ON test_ks.test_table (asciiColumn) USING 'org.apache.cassandra.index.sasi.SASIIndex'"
      )

    }

    /*
    "add colums" in {

      case class DummyClass(
        name: String
        , height: String
      )

      val generic = LabelledGeneric[DummyClass]

      val tableDef = "CREATE TABLE test_ks.test_table (name text,height text,intColumn int,longColumn bigint,stringColumn text,asciiColumn ascii,floatColumn float,doubleColumn double,bigDecimalColumn decimal,bigIntColumn varint,blobColumn blob,uuidColumn uuid,timeUuidColumn timeuuid,durationColumn bigint,inetAddressColumn inet,enumColumn text, PRIMARY KEY ((intColumn)))"

      ks.table[SimpleTableRow]
      .partition(Symbol("intColumn"))
      .columns[generic.Repr]
      .build("test_table").cqlStatement shouldBe Seq(tableDef)
    }
     */

  }


  "DDL for table with options with " - {


    val tableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,boolColumn boolean,maybeIntColumn int,stringColumn text,asciiColumn ascii,enumColumn text,listColumn list<text>,setColumn set<text>,vectorColumn list<text>,"

    "partition key" in {
      val table =
        ks.table[OptionalTableRow]
          .partition(Symbol("intColumn"))
          .build("test_table")


      table.cqlStatement shouldBe Seq(s"$tableDef PRIMARY KEY ((intColumn)))")
    }


  }


  "DDL for table with List/Seq/Set/Vector with " - {


    val tableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,listColumn list<text>,setColumn set<text>,vectorColumn list<text>,seqColumn list<text>,"


    "partition key" in {
      val table =
        ks.table[ListTableRow]
          .partition(Symbol("intColumn"))
          .build("test_table")



      table.cqlStatement shouldBe  Seq(s"$tableDef PRIMARY KEY ((intColumn)))")
    }

  }



  "DDL for table with tuples with " - {


    val tableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,tuple2Column frozen<tuple<text, int>>,tuple3Column frozen<tuple<text, ascii, bigint>>,tuple4Column frozen<tuple<text, ascii, uuid, timeuuid>>,tuple5Column frozen<tuple<text, ascii, uuid, timeuuid, timestamp>>,"


    "partition key" in {
      val table =
        ks.table[TupleTableRow]
          .partition(Symbol("intColumn"))
          .build("test_table")


      table.cqlStatement shouldBe  Seq(s"$tableDef PRIMARY KEY ((intColumn)))")
    }


  }


  "DDL for table with maps with " - {


    val tableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,mapStringColumn map<text, text>,mapIntColumn map<int, text>,"

    "partition key" in {
      val table =
        ks.table[MapTableRow]
          .partition(Symbol("intColumn"))
          .build("test_table")



      table.cqlStatement shouldBe Seq(s"$tableDef PRIMARY KEY ((intColumn)))")
    }

  }


  "DDL for table with constant size Vectors with " - {


    val tableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,vector4IntColumn vector<int,4>,vector8FloatColumn vector<float,8>,"


    "partition key" in {
      val table =
        ks.table[VectorTableRow]
          .partition(Symbol("intColumn"))
          .build("test_table")



      table.cqlStatement shouldBe  Seq(s"$tableDef PRIMARY KEY ((intColumn)))")
    }

  }

  "DDL for table with SAI indexes with " - {

    val simpleTableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,stringColumn text,asciiColumn ascii,floatColumn float,doubleColumn double,bigDecimalColumn decimal,bigIntColumn varint,blobColumn blob,uuidColumn uuid,timeUuidColumn timeuuid,durationColumn bigint,inetAddressColumn inet,enumColumn text,"

    "SAI index" in {
      val table =
        ks.table[SimpleTableRow]
          .partition(Symbol("intColumn"))
          .indexBySAI(Symbol("asciiColumn"), "ascii_sai_idx")
          .build("test_table")

      table.cqlStatement.toSet shouldBe Set(
        s"$simpleTableDef PRIMARY KEY ((intColumn)))"
        , "CREATE CUSTOM INDEX ascii_sai_idx ON test_ks.test_table (asciiColumn) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'"
      )
    }

    "SAI index with options" in {
      val table =
        ks.table[SimpleTableRow]
          .partition(Symbol("intColumn"))
          .indexBySAI(Symbol("stringColumn"), "string_sai_idx", Map("case_sensitive" -> "false", "normalize" -> "true"))
          .build("test_table")

      table.cqlStatement.toSet shouldBe Set(
        s"$simpleTableDef PRIMARY KEY ((intColumn)))"
        , "CREATE CUSTOM INDEX string_sai_idx ON test_ks.test_table (stringColumn) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' WITH OPTIONS = {'case_sensitive': 'false','normalize': 'true'}"
      )
    }

    "SAI vector index with cosine similarity" in {
      val vectorTableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,vector4IntColumn vector<int,4>,vector8FloatColumn vector<float,8>,"

      val table =
        ks.table[VectorTableRow]
          .partition(Symbol("intColumn"))
          .indexBySAIVector(Symbol("vector8FloatColumn"), "vector_sai_idx", SimilarityFunction.COSINE)
          .build("test_table")

      table.cqlStatement.toSet shouldBe Set(
        s"$vectorTableDef PRIMARY KEY ((intColumn)))"
        , "CREATE CUSTOM INDEX vector_sai_idx ON test_ks.test_table (vector8FloatColumn) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'COSINE'}"
      )
    }

    "SAI vector index with dot product similarity" in {
      val vectorTableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,vector4IntColumn vector<int,4>,vector8FloatColumn vector<float,8>,"

      val table =
        ks.table[VectorTableRow]
          .partition(Symbol("intColumn"))
          .indexBySAIVector(Symbol("vector8FloatColumn"), "vector_sai_idx", SimilarityFunction.DOT_PRODUCT)
          .build("test_table")

      table.cqlStatement.toSet shouldBe Set(
        s"$vectorTableDef PRIMARY KEY ((intColumn)))"
        , "CREATE CUSTOM INDEX vector_sai_idx ON test_ks.test_table (vector8FloatColumn) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'DOT_PRODUCT'}"
      )
    }

    "SAI collection index with KEYS" in {
      val mapTableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,mapStringColumn map<text, text>,mapIntColumn map<int, text>,"

      val table =
        ks.table[MapTableRow]
          .partition(Symbol("intColumn"))
          .indexBySAICollection(Symbol("mapStringColumn"), "map_keys_idx", CollectionIndexTarget.Keys)
          .build("test_table")

      table.cqlStatement.toSet shouldBe Set(
        s"$mapTableDef PRIMARY KEY ((intColumn)))"
        , "CREATE CUSTOM INDEX map_keys_idx ON test_ks.test_table (KEYS(mapStringColumn)) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'"
      )
    }

    "SAI collection index with VALUES" in {
      val listTableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,listColumn list<text>,setColumn set<text>,vectorColumn list<text>,seqColumn list<text>,"

      val table =
        ks.table[ListTableRow]
          .partition(Symbol("intColumn"))
          .indexBySAICollection(Symbol("listColumn"), "list_values_idx", CollectionIndexTarget.Values)
          .build("test_table")

      table.cqlStatement.toSet shouldBe Set(
        s"$listTableDef PRIMARY KEY ((intColumn)))"
        , "CREATE CUSTOM INDEX list_values_idx ON test_ks.test_table (VALUES(listColumn)) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'"
      )
    }

    "SAI collection index with ENTRIES" in {
      val mapTableDef = "CREATE TABLE test_ks.test_table (intColumn int,longColumn bigint,mapStringColumn map<text, text>,mapIntColumn map<int, text>,"

      val table =
        ks.table[MapTableRow]
          .partition(Symbol("intColumn"))
          .indexBySAICollection(Symbol("mapStringColumn"), "map_entries_idx", CollectionIndexTarget.Entries)
          .build("test_table")

      table.cqlStatement.toSet shouldBe Set(
        s"$mapTableDef PRIMARY KEY ((intColumn)))"
        , "CREATE CUSTOM INDEX map_entries_idx ON test_ks.test_table (ENTRIES(mapStringColumn)) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'"
      )
    }
  }


}
