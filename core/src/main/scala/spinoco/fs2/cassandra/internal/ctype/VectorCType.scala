package spinoco.fs2.cassandra.internal.ctype

import cats.effect.Async
import com.datastax.oss.driver.api.core.`type`.{DataType, DataTypes, VectorType}
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, Row}
import com.datastax.oss.driver.api.core.data.CqlVector
import com.datastax.oss.driver.api.core.detach.AttachmentPoint
import fs2.Stream
import spinoco.fs2.cassandra.CType
import shapeless.tag
import shapeless.tag.@@
import spinoco.fs2.cassandra.internal.ctype.CollectionCType.ConstDimension
import spinoco.fs2.cassandra.util.CompletionStageSyntax

import java.util.concurrent.CompletionStage


object VectorCType {
  def vectorDataType(elementType: DataType, dimension: Int): DataType = {
    new VectorType {
      private val subtype: DataType = elementType
      private val dimensions: Int = dimension

      override def getDimensions: Int = dimensions

      override def getElementType: DataType = subtype

      override def getClassName: String = "org.apache.cassandra.db.marshal.VectorType"

      override def asCql(includeFrozen: Boolean, pretty: Boolean): String =
        s"vector<${subtype.asCql(true, false)},${dimension}>"

      override def equals(o: Any): Boolean = o match {
        case that: VectorType =>
          that.getElementType.equals(this.getElementType()) &&
            that.getDimensions == this.getDimensions()
        case _ => false
      }

      override def hashCode(): Int =
        java.util.Objects.hash(getElementType(), Integer.valueOf(getDimensions()))

      override def toString: String =
        s"vector<${subtype.asCql(true, false)},${dimension}>"

      override def isDetached: Boolean = false

      override def attach(attachmentPoint: AttachmentPoint): Unit = {}
    }
  }

  def instance[A : CType : Numeric, T](dimension: Int):CType[Vector[A] @@ T] = {
    CollectionCType.instance[Vector, A](
      dimension = Some(ConstDimension(dimension = dimension, tpe = vectorDataType(CType[A].cqlType, dimension)))
    ).xmap[Vector[A] @@ T](vecWoTag => tag[T][Vector[A]](vecWoTag), identity)
  }
}
