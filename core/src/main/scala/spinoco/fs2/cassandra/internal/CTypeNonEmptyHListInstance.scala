package spinoco.fs2.cassandra.internal


import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.data.{GettableByIndex, SettableByIndex}
import shapeless.{::, HList, HNil}

import spinoco.fs2.cassandra.CType
import spinoco.fs2.cassandra.internal.CodecSerializer.CodecSerializeSyntax
import spinoco.fs2.cassandra.internal.CodecWriter.CodecWriteSyntax


/**
  * Created by pach on 07/06/16.
  */
trait CTypeNonEmptyHListInstance[R <: HList] {
  type CTypes
  def types:Seq[DataType]
  def write[D <: SettableByIndex[D]](r:R, data:D, protocolVersion: ProtocolVersion):D
  def writeAt[D <: SettableByIndex[D]](r:R, idx:Int, data:D, protocolVersion: ProtocolVersion):D
  def readAt(idx:Int, data:GettableByIndex, protocolVersion: ProtocolVersion):Either[Throwable,R]
  def read(data:GettableByIndex, protocolVersion: ProtocolVersion):Either[Throwable,R]

}

object CTypeNonEmptyHListInstance {

  @inline def apply[R <: HList](implicit inst: CTypeNonEmptyHListInstance[R]): CTypeNonEmptyHListInstance[R] = inst

  type Aux[R<: HList, CT <: HList] = CTypeNonEmptyHListInstance[R] { type CTypes = CT }

  implicit def tailInstance[V](
    implicit
    CT: CType[V]
  ):CTypeNonEmptyHListInstance.Aux[V :: HNil, CType[V] :: HNil] = {
    new CTypeNonEmptyHListInstance[V :: HNil] {
      type CTypes =  CType[V] :: HNil
      def types:Seq[DataType] = Seq(CT.cqlType)
      def write[D <: SettableByIndex[D]](r:V:: HNil, data:D, protocolVersion: ProtocolVersion): D =
        writeAt(r,0,data,protocolVersion)

      def writeAt[D <: SettableByIndex[D]](r:V:: HNil, idx:Int, data:D, protocolVersion: ProtocolVersion): D = {
        CT.writeAtSerialized(idx, r.head, data, protocolVersion)
      }

      def readAt(idx:Int, data:GettableByIndex, protocolVersion: ProtocolVersion):Either[Throwable,V :: HNil] = {
        CT.deserialize(data.getBytesUnsafe(idx), protocolVersion)
          .right.map(_ :: HNil)
      }

      def read(data:GettableByIndex, protocolVersion: ProtocolVersion):Either[Throwable,V:: HNil] =
        readAt(0,data,protocolVersion)
    }
  }

  implicit def instance[V,L <: HList, C <: HList](
    implicit
    tail: CTypeNonEmptyHListInstance.Aux[L,C]
    , CT: CType[V]
  ):CTypeNonEmptyHListInstance.Aux[V :: L, CType[V] :: C] = {
    new CTypeNonEmptyHListInstance[V :: L] {
      type CTypes = CType[V] :: C

      def types: Seq[DataType] = CT.cqlType +: tail.types

      def writeAt[D <: SettableByIndex[D]](r: ::[V, L], idx: Int, data: D, protocolVersion: ProtocolVersion): D = {
        val serialized = CT.writeAtSerialized(idx, r.head, data, protocolVersion)
        tail.writeAt(r.tail, idx + 1, serialized, protocolVersion)
      }

      def write[D <: SettableByIndex[D]](r: ::[V, L], data: D, protocolVersion: ProtocolVersion): D = writeAt(r, 0, data, protocolVersion)

      def read(data: GettableByIndex, protocolVersion: ProtocolVersion): Either[Throwable, ::[V, L]] = readAt(0, data, protocolVersion)

      def readAt(idx: Int, data: GettableByIndex, protocolVersion: ProtocolVersion): Either[Throwable, ::[V, L]] = {
        tail.readAt(idx + 1, data, protocolVersion).right.flatMap { t =>
          CT.deserialize(data.getBytesUnsafe(idx), protocolVersion).right.map(_ :: t)
        }
      }
    }
  }
}