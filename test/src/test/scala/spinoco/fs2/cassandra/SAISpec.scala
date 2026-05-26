package spinoco.fs2.cassandra

import fs2.Stream._
import shapeless.tag
import shapeless.tag.@@
import spinoco.fs2.cassandra.builder.SimilarityFunction
import spinoco.fs2.cassandra.sample.VectorSizes._
import spinoco.fs2.cassandra.sample.{SimpleTableRow, VectorTableRow}


class SAISpec extends SchemaSupport {

  def withCassandra5(f: CassandraSession[cats.effect.IO] => Any): Unit =
    withSessionFor(_.startsWith("5"))(f)

  "SAI index features" - {

    "create table with SAI index" in withCassandra5 { cs =>
      val table =
        ks.table[SimpleTableRow]
          .partition(Symbol("intColumn"))
          .cluster(Symbol("longColumn"))
          .indexBySAI(Symbol("asciiColumn"), "ascii_sai_idx")
          .build("sai_simple_table")

      (for {
        _ <- cs.create(ks)
        _ <- cs.create(table)
      } yield ()).unsafeRunSync()

      val query = system.schema.queryAllTables.map(t => t.keyspace_name -> t.table_name)
      val result = cs.queryAll(query).compile.toVector.unsafeRunSync()
      result should contain(ks.name -> "sai_simple_table")
    }

    "query by SAI index with EQ" in withCassandra5 { cs =>
      val table =
        ks.table[SimpleTableRow]
          .partition(Symbol("intColumn"))
          .cluster(Symbol("longColumn"))
          .indexBySAI(Symbol("stringColumn"), "string_sai_idx")
          .build("sai_eq_table")

      (for {
        _ <- cs.create(ks)
        _ <- cs.create(table)
      } yield ()).unsafeRunSync()

      val insert = table.insert.all.build.from[SimpleTableRow]
      val entries = for (i <- 0 to 5; l <- 0L to 2L) yield
        SimpleTableRow.simpleInstance.copy(
          intColumn = i, longColumn = l,
          stringColumn = if (i % 2 == 0) "even" else "odd"
        )
      emits(entries).flatMap(e => eval(cs.execute(insert)(e)).drain).compile.drain.unsafeRunSync()

      val query = table.query.all
        .byIndex(Symbol("stringColumn"), Comparison.EQ)
        .allowFiltering
        .build
        .fromA[String]
        .as[SimpleTableRow]

      val result = cs.query(query)("even").compile.toVector.unsafeRunSync()
      result.foreach(_.stringColumn shouldBe "even")
      result should not be empty
    }

    "query by SAI index with range" in withCassandra5 { cs =>
      val table =
        ks.table[SimpleTableRow]
          .partition(Symbol("intColumn"))
          .cluster(Symbol("longColumn"))
          .indexBySAI(Symbol("floatColumn"), "float_sai_idx")
          .build("sai_range_table")

      (for {
        _ <- cs.create(ks)
        _ <- cs.create(table)
      } yield ()).unsafeRunSync()

      val insert = table.insert.all.build.from[SimpleTableRow]
      val entries = for (i <- 0 to 3) yield
        SimpleTableRow.simpleInstance.copy(intColumn = i, longColumn = 1L, floatColumn = i.toFloat * 10.0f)
      emits(entries).flatMap(e => eval(cs.execute(insert)(e)).drain).compile.drain.unsafeRunSync()

      val query = table.query.all
        .byIndex(Symbol("floatColumn"), Comparison.GTEQ)
        .allowFiltering
        .build
        .fromA[Float]
        .as[SimpleTableRow]

      val result = cs.query(query)(20.0f).compile.toVector.unsafeRunSync()
      result.foreach(_.floatColumn should be >= 20.0f)
      result should not be empty
    }

  }


  "ANN vector search" - {

    "ORDER BY ANN OF" in withCassandra5 { cs =>
      val table =
        ks.table[VectorTableRow]
          .partition(Symbol("intColumn"))
          .indexBySAIVector(Symbol("vector8FloatColumn"), "vec_ann_idx", SimilarityFunction.COSINE)
          .build("ann_table")

      (for {
        _ <- cs.create(ks)
        _ <- cs.create(table)
      } yield ()).unsafeRunSync()

      val insert = table.insert.all.build.from[VectorTableRow]
      val entries = (1 to 5).map { i =>
        VectorTableRow.instance.copy(
          intColumn = i
          , longColumn = i.toLong
          , vector8FloatColumn = tag[VectorSize8](Vector.fill(8)(i.toFloat / 10.0f))
        )
      }
      emits(entries).flatMap(e => eval(cs.execute(insert)(e)).drain).compile.drain.unsafeRunSync()

      val query = table.query
        .column(Symbol("intColumn"))
        .column(Symbol("longColumn"))
        .orderByAnn(Symbol("vector8FloatColumn"))
        .limit(3)
        .build
        .fromA[Vector[Float] @@ VectorSize8]
        .asTuple

      val queryVec = tag[VectorSize8](Vector.fill(8)(0.3f))
      val result = cs.query(query)(queryVec).compile.toVector.unsafeRunSync()

      result.size shouldBe 3
    }

    "ANN with partition key" in withCassandra5 { cs =>
      val table =
        ks.table[VectorTableRow]
          .partition(Symbol("intColumn"))
          .indexBySAIVector(Symbol("vector8FloatColumn"), "vec_pk_ann_idx", SimilarityFunction.COSINE)
          .build("ann_pk_table")

      (for {
        _ <- cs.create(ks)
        _ <- cs.create(table)
      } yield ()).unsafeRunSync()

      val insert = table.insert.all.build.from[VectorTableRow]
      val entries = (1 to 5).map { i =>
        VectorTableRow.instance.copy(
          intColumn = 1
          , longColumn = i.toLong
          , vector8FloatColumn = tag[VectorSize8](Vector.fill(8)(i.toFloat / 10.0f))
        )
      }
      emits(entries).flatMap(e => eval(cs.execute(insert)(e)).drain).compile.drain.unsafeRunSync()

      val query = table.query
        .column(Symbol("intColumn"))
        .column(Symbol("longColumn"))
        .partition
        .orderByAnn(Symbol("vector8FloatColumn"))
        .limit(3)
        .build
        .fromHList
        .fromTuple[(Int, Vector[Float] @@ VectorSize8)]
        .asTuple

      val queryVec = tag[VectorSize8](Vector.fill(8)(0.3f))
      val result = cs.query(query)((1, queryVec)).compile.toVector.unsafeRunSync()

      result should not be empty
      result.size should be <= 3
    }

    "similarity_cosine function with ANN" in withCassandra5 { cs =>
      val table =
        ks.table[VectorTableRow]
          .partition(Symbol("intColumn"))
          .indexBySAIVector(Symbol("vector8FloatColumn"), "vec_sim_cos_idx", SimilarityFunction.COSINE)
          .build("sim_cos_table")

      (for {
        _ <- cs.create(ks)
        _ <- cs.create(table)
      } yield ()).unsafeRunSync()

      val insert = table.insert.all.build.from[VectorTableRow]
      val entries = (1 to 3).map { i =>
        VectorTableRow.instance.copy(
          intColumn = i
          , longColumn = i.toLong
          , vector8FloatColumn = tag[VectorSize8](Vector.fill(8)(i.toFloat / 10.0f))
        )
      }
      emits(entries).flatMap(e => eval(cs.execute(insert)(e)).drain).compile.drain.unsafeRunSync()

      val query = table.query
        .column(Symbol("intColumn"))
        .orderByAnn(Symbol("vector8FloatColumn"))
        .function2AtShared(functions.similarityCosine[Float, VectorSize8], Symbol("vector8FloatColumn"), Symbol("vector8FloatColumn"), Symbol("score"))
        .limit(3)
        .build
        .fromA[Vector[Float] @@ VectorSize8]
        .asTuple

      val queryVec = tag[VectorSize8](Vector.fill(8)(0.1f))
      val result = cs.query(query)(queryVec).compile.toVector.unsafeRunSync()

      result.size shouldBe 3
      result.foreach { case (score, _) => score should (be >= 0.0f and be <= 1.0f) }
    }

    "similarity_euclidean function with ANN" in withCassandra5 { cs =>
      val table =
        ks.table[VectorTableRow]
          .partition(Symbol("intColumn"))
          .indexBySAIVector(Symbol("vector8FloatColumn"), "vec_sim_euc_idx", SimilarityFunction.EUCLIDEAN)
          .build("sim_euc_table")

      (for {
        _ <- cs.create(ks)
        _ <- cs.create(table)
      } yield ()).unsafeRunSync()

      val insert = table.insert.all.build.from[VectorTableRow]
      val entries = (1 to 3).map { i =>
        VectorTableRow.instance.copy(
          intColumn = i
          , longColumn = i.toLong
          , vector8FloatColumn = tag[VectorSize8](Vector.fill(8)(i.toFloat / 10.0f))
        )
      }
      emits(entries).flatMap(e => eval(cs.execute(insert)(e)).drain).compile.drain.unsafeRunSync()

      val query = table.query
        .column(Symbol("intColumn"))
        .orderByAnn(Symbol("vector8FloatColumn"))
        .function2AtShared(functions.similarityEuclidean[Float, VectorSize8], Symbol("vector8FloatColumn"), Symbol("vector8FloatColumn"), Symbol("score"))
        .limit(3)
        .build
        .fromA[Vector[Float] @@ VectorSize8]
        .asTuple

      val queryVec = tag[VectorSize8](Vector.fill(8)(0.1f))
      val result = cs.query(query)(queryVec).compile.toVector.unsafeRunSync()

      result.size shouldBe 3
      result.foreach { case (score, _) => score should (be >= 0.0f and be <= 1.0f) }
    }

    "similarity_dot_product function with ANN" in withCassandra5 { cs =>
      val table =
        ks.table[VectorTableRow]
          .partition(Symbol("intColumn"))
          .indexBySAIVector(Symbol("vector8FloatColumn"), "vec_sim_dot_idx", SimilarityFunction.DOT_PRODUCT)
          .build("sim_dot_table")

      (for {
        _ <- cs.create(ks)
        _ <- cs.create(table)
      } yield ()).unsafeRunSync()

      val insert = table.insert.all.build.from[VectorTableRow]
      // Use normalized vectors for dot product (magnitudes close to 1)
      val entries = (1 to 3).map { i =>
        val mag = math.sqrt(8.0 * (i.toFloat / 10.0f) * (i.toFloat / 10.0f)).toFloat
        val norm = if (mag > 0) i.toFloat / 10.0f / mag else 0f
        VectorTableRow.instance.copy(
          intColumn = i
          , longColumn = i.toLong
          , vector8FloatColumn = tag[VectorSize8](Vector.fill(8)(norm))
        )
      }
      emits(entries).flatMap(e => eval(cs.execute(insert)(e)).drain).compile.drain.unsafeRunSync()

      val query = table.query
        .column(Symbol("intColumn"))
        .orderByAnn(Symbol("vector8FloatColumn"))
        .function2AtShared(functions.similarityDotProduct[Float, VectorSize8], Symbol("vector8FloatColumn"), Symbol("vector8FloatColumn"), Symbol("score"))
        .limit(3)
        .build
        .fromA[Vector[Float] @@ VectorSize8]
        .asTuple

      val norm = (1.0f / math.sqrt(8.0)).toFloat
      val queryVec = tag[VectorSize8](Vector.fill(8)(norm))
      val result = cs.query(query)(queryVec).compile.toVector.unsafeRunSync()

      result.size shouldBe 3
      result.foreach { case (score, _) => score should (be >= 0.0f and be <= 1.0f) }
    }
  }
}
