package spinoco.fs2.cassandra.builder

import shapeless.LabelledGeneric
import spinoco.fs2.cassandra.sample.{ListTableRow, SimpleTableRow, VectorTableRow}
import spinoco.fs2.cassandra.sample.VectorSizes._
import spinoco.fs2.cassandra.support.Fs2CassandraSpec
import spinoco.fs2.cassandra.{Comparison, KeySpace, functions}


class QueryBuilderSpec extends Fs2CassandraSpec {

  val ks = new KeySpace("test_ks")

  "SELECT statement" - {

    val simpleTable =
      ks.table[SimpleTableRow]
        .partition(Symbol("intColumn"))
        .cluster(Symbol("longColumn"))
        .build("test_table")

    val simpleTableCompoundPk =
      ks.table[SimpleTableRow]
        .partition(Symbol("intColumn"))
        .partition(Symbol("longColumn"))
        .cluster(Symbol("stringColumn"))
        .build("test_table")

    val simpleTableCompoundCk =
      ks.table[SimpleTableRow]
        .partition(Symbol("intColumn"))
        .cluster(Symbol("longColumn"))
        .cluster(Symbol("stringColumn"))
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
      .column(Symbol("stringColumn"))
      .build
      .cqlStatement shouldBe
        "SELECT stringColumn" +
        " FROM test_ks.test_table"

    }

    "will select single column with alias from given row" in {
      simpleTable.query
        .columnAs(Symbol("stringColumn"), "as_alias")
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
        .functionAt(functions.dateOf, Symbol("timeUuidColumn"), "time_of_uuid")
        .build
        .cqlStatement shouldBe
        "SELECT dateOf(timeUuidColumn) AS time_of_uuid" +
          " FROM test_ks.test_table"

    }

    "will select timestamp of" in {
      simpleTable.query
        .functionAt(functions.unixTimestampOf, Symbol("timeUuidColumn"), "timestamp_of_uuid")
        .build
        .cqlStatement shouldBe
        "SELECT unixTimestampOf(timeUuidColumn) AS timestamp_of_uuid" +
          " FROM test_ks.test_table"

    }

    "will select write time of" in {
      simpleTable.query
        .functionAt(functions.writeTimeOfMicro[String], Symbol("stringColumn"), "write_time_of")
        .build
        .cqlStatement shouldBe
        "SELECT WRITETIME(stringColumn) AS write_time_of" +
          " FROM test_ks.test_table"

    }


    "will select ttl of" in {
      simpleTable.query
        .functionAt(functions.ttlOf[String], Symbol("stringColumn"), "ttl_of")
        .build
        .cqlStatement shouldBe
        "SELECT TTL(stringColumn) AS ttl_of" +
          " FROM test_ks.test_table"

    }


    "will select with partitioning key" in {

      simpleTable.query
      .column(Symbol("stringColumn"))
      .partition
      .build
      .cqlStatement shouldBe
      "SELECT stringColumn FROM test_ks.test_table" +
        " WHERE intColumn = :intColumn"

    }

    "will select with partitioning key for compound key" in {

      simpleTableCompoundPk.query
        .column(Symbol("stringColumn"))
        .partition
        .build
        .cqlStatement shouldBe
        "SELECT stringColumn FROM test_ks.test_table" +
          " WHERE intColumn = :intColumn AND longColumn = :longColumn"

    }


    "will select with partitioning key and cluster key" in {

      simpleTable.query
        .column(Symbol("stringColumn"))
        .partition
        .cluster(Symbol("longColumn"), Comparison.GTEQ)
        .build
        .cqlStatement shouldBe
        "SELECT stringColumn FROM test_ks.test_table" +
          " WHERE intColumn = :intColumn AND longColumn >= :longColumn"

    }

    "will select with partitioning key and cluster key (aliased)" in {
      simpleTable.query
        .column(Symbol("stringColumn"))
        .partition
        .cluster(Symbol("longColumn"), "greater", Comparison.GTEQ)
        .build
        .cqlStatement shouldBe
        "SELECT stringColumn FROM test_ks.test_table" +
          " WHERE intColumn = :intColumn AND longColumn >= :greater"

    }

    "will select with partitioning key and compound cluster key" in {
      simpleTableCompoundCk.query
        .column(Symbol("stringColumn"))
        .partition
        .cluster(Symbol("longColumn"), "greaterLong", Comparison.GTEQ)
        .cluster(Symbol("stringColumn"), "greaterString", Comparison.GTEQ)
        .build
        .cqlStatement shouldBe
        "SELECT stringColumn FROM test_ks.test_table" +
          " WHERE intColumn = :intColumn AND (longColumn,stringColumn) >= (:greaterLong,:greaterString)"

    }


    "will select with limit" in {
      simpleTable.query
      .column(Symbol("stringColumn"))
      .limit(1)
      .build
      .cqlStatement shouldBe
      "SELECT stringColumn" +
        " FROM test_ks.test_table LIMIT 1"
    }


    "will select with allow filtering" in {
      simpleTable.query
        .column(Symbol("stringColumn"))
        .allowFiltering
        .build
        .cqlStatement shouldBe
        "SELECT stringColumn" +
          " FROM test_ks.test_table ALLOW FILTERING"
    }


    "will select with order by asc" in {
      simpleTable.query
        .column(Symbol("stringColumn"))
        .orderBy(Symbol("longColumn"), ascending = true)
        .build
        .cqlStatement shouldBe
        "SELECT stringColumn" +
          " FROM test_ks.test_table ORDER BY longColumn ASC"
    }


    "will select with order by desc" in {
      simpleTable.query
        .column(Symbol("stringColumn"))
        .orderBy(Symbol("longColumn"), ascending = false)
        .build
        .cqlStatement shouldBe
        "SELECT stringColumn" +
          " FROM test_ks.test_table ORDER BY longColumn DESC"
    }

    "will fill in cqlFor" in {
      simpleTableCompoundPk.query
        .column(Symbol("stringColumn"))
        .partition
        .build
        .fromHList
        .fromTuple[(Int, Long)]
        .cqlFor((1, 2L)) shouldBe
        "SELECT stringColumn FROM test_ks.test_table" +
          " WHERE intColumn = 1 AND longColumn = 2"

    }

    "will select columns of list" in {
      val generic = LabelledGeneric[SimpleTableRow]
      simpleTable.query
      .columns[generic.Repr]
      .build
      .cqlStatement shouldBe
        "SELECT intColumn,longColumn,stringColumn,asciiColumn,floatColumn,doubleColumn,bigDecimalColumn,bigIntColumn,blobColumn,uuidColumn,timeUuidColumn,durationColumn,inetAddressColumn,enumColumn" +
          " FROM test_ks.test_table"
    }

  }


  "SAI query features" - {

    val vectorTable =
      ks.table[VectorTableRow]
        .partition(Symbol("intColumn"))
        .cluster(Symbol("longColumn"))
        .indexBySAIVector(Symbol("vector8FloatColumn"), "vector_sai_idx")
        .build("test_table")

    val indexedSimpleTable =
      ks.table[SimpleTableRow]
        .partition(Symbol("intColumn"))
        .cluster(Symbol("longColumn"))
        .indexBySAI(Symbol("stringColumn"), "string_idx")
        .build("test_table")

    val indexedListTable =
      ks.table[ListTableRow]
        .partition(Symbol("intColumn"))
        .cluster(Symbol("longColumn"))
        .indexBySAICollection(Symbol("listColumn"), "list_idx", CollectionIndexTarget.Values)
        .build("test_table")

    "will select with ANN ORDER BY" in {
      vectorTable.query
        .all
        .partition
        .orderByAnn(Symbol("vector8FloatColumn"))
        .limit(10)
        .build
        .cqlStatement shouldBe
        "SELECT intColumn,longColumn,vector4IntColumn,vector8FloatColumn FROM test_ks.test_table" +
          " WHERE intColumn = :intColumn ORDER BY vector8FloatColumn ANN OF :vector8FloatColumn LIMIT 10"
    }

    "will select with ANN ORDER BY aliased" in {
      vectorTable.query
        .all
        .partition
        .orderByAnn(Symbol("vector8FloatColumn"), Symbol("queryVec"))
        .limit(10)
        .build
        .cqlStatement shouldBe
        "SELECT intColumn,longColumn,vector4IntColumn,vector8FloatColumn FROM test_ks.test_table" +
          " WHERE intColumn = :intColumn ORDER BY vector8FloatColumn ANN OF :queryVec LIMIT 10"
    }

    "will select with similarity function" in {
      vectorTable.query
        .function2At(functions.similarityCosine[Float, VectorSize8], Symbol("vector8FloatColumn"), Symbol("queryVec"), Symbol("score"))
        .partition
        .build
        .cqlStatement shouldBe
        "SELECT similarity_cosine(vector8FloatColumn, :queryVec) AS score FROM test_ks.test_table" +
          " WHERE intColumn = :intColumn"
    }

    "will select with ANN and shared similarity function" in {
      vectorTable.query
        .all
        .partition
        .orderByAnn(Symbol("vector8FloatColumn"))
        .function2AtShared(functions.similarityCosine[Float, VectorSize8], Symbol("vector8FloatColumn"), Symbol("vector8FloatColumn"), Symbol("score"))
        .limit(10)
        .build
        .cqlStatement shouldBe
        "SELECT intColumn,longColumn,vector4IntColumn,vector8FloatColumn,similarity_cosine(vector8FloatColumn, :vector8FloatColumn) AS score FROM test_ks.test_table" +
          " WHERE intColumn = :intColumn ORDER BY vector8FloatColumn ANN OF :vector8FloatColumn LIMIT 10"
    }

    "will select with similarity euclidean" in {
      vectorTable.query
        .function2At(functions.similarityEuclidean[Float, VectorSize8], Symbol("vector8FloatColumn"), Symbol("queryVec"), Symbol("score"))
        .partition
        .build
        .cqlStatement shouldBe
        "SELECT similarity_euclidean(vector8FloatColumn, :queryVec) AS score FROM test_ks.test_table" +
          " WHERE intColumn = :intColumn"
    }

    "will select with similarity dot product" in {
      vectorTable.query
        .function2At(functions.similarityDotProduct[Float, VectorSize8], Symbol("vector8FloatColumn"), Symbol("queryVec"), Symbol("score"))
        .partition
        .build
        .cqlStatement shouldBe
        "SELECT similarity_dot_product(vector8FloatColumn, :queryVec) AS score FROM test_ks.test_table" +
          " WHERE intColumn = :intColumn"
    }

    "will select with byIndex CONTAINS" in {
      indexedListTable.query
        .all
        .partition
        .byIndex(Symbol("listColumn"), Comparison.CONTAINS)
        .build
        .cqlStatement shouldBe
        "SELECT intColumn,longColumn,listColumn,setColumn,vectorColumn,seqColumn FROM test_ks.test_table" +
          " WHERE intColumn = :intColumn AND listColumn CONTAINS :listColumn"
    }

    "will select with byIndex EQ on SAI" in {
      indexedSimpleTable.query
        .all
        .partition
        .byIndex(Symbol("stringColumn"), Comparison.EQ)
        .build
        .cqlStatement shouldBe
        "SELECT intColumn,longColumn,stringColumn,asciiColumn,floatColumn,doubleColumn,bigDecimalColumn,bigIntColumn,blobColumn,uuidColumn,timeUuidColumn,durationColumn,inetAddressColumn,enumColumn FROM test_ks.test_table" +
          " WHERE intColumn = :intColumn AND stringColumn = :stringColumn"
    }

    "will select with byIndexIn" in {
      indexedSimpleTable.query
        .all
        .partition
        .byIndexIn(Symbol("stringColumn"))
        .build
        .cqlStatement shouldBe
        "SELECT intColumn,longColumn,stringColumn,asciiColumn,floatColumn,doubleColumn,bigDecimalColumn,bigIntColumn,blobColumn,uuidColumn,timeUuidColumn,durationColumn,inetAddressColumn,enumColumn FROM test_ks.test_table" +
          " WHERE intColumn = :intColumn AND stringColumn IN :stringColumn"
    }

    "will select with byIndexIn aliased" in {
      indexedSimpleTable.query
        .all
        .partition
        .byIndexIn(Symbol("stringColumn"), Symbol("statusList"))
        .build
        .cqlStatement shouldBe
        "SELECT intColumn,longColumn,stringColumn,asciiColumn,floatColumn,doubleColumn,bigDecimalColumn,bigIntColumn,blobColumn,uuidColumn,timeUuidColumn,durationColumn,inetAddressColumn,enumColumn FROM test_ks.test_table" +
          " WHERE intColumn = :intColumn AND stringColumn IN :statusList"
    }

  }


}
