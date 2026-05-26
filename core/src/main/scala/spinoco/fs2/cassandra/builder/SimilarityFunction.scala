package spinoco.fs2.cassandra.builder

sealed abstract class SimilarityFunction(val name: String)

object SimilarityFunction {
  case object COSINE      extends SimilarityFunction("COSINE")
  case object DOT_PRODUCT extends SimilarityFunction("DOT_PRODUCT")
  case object EUCLIDEAN   extends SimilarityFunction("EUCLIDEAN")
}
