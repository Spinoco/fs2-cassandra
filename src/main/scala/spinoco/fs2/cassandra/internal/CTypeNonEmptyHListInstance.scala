package spinoco.fs2.cassandra.internal


import com.datastax.driver.core.{DataType, GettableByIndexData, ProtocolVersion, SettableByIndexData}
import shapeless.{::, HList, HNil}
import spinoco.fs2.cassandra.CType

/**
  * Created by pach on 07/06/16.
  */
trait CTypeNonEmptyHListInstance[R <: HList] {
  type CTypes
  def types:Seq[DataType]
  def write(r:R, data:SettableByIndexData[_], protocolVersion: ProtocolVersion):Unit
  def writeAt(r:R, idx:Int, data:SettableByIndexData[_], protocolVersion: ProtocolVersion):Unit
  def readAt(idx:Int, data:GettableByIndexData, protocolVersion: ProtocolVersion):Either[Throwable,R]
  def read(data:GettableByIndexData, protocolVersion: ProtocolVersion):Either[Throwable,R]
}


object CTypeNonEmptyHListInstance {

  type Aux[R<: HList, CT <: HList] = CTypeNonEmptyHListInstance[R] { type CTypes = CT }

  implicit def tailInstance[V](
    implicit
    CT: CType[V]
  ):CTypeNonEmptyHListInstance.Aux[V :: HNil, CType[V] :: HNil] = {
    new CTypeNonEmptyHListInstance[V :: HNil] {
      type CTypes =  CType[V] :: HNil
      def types:Seq[DataType] = Seq(CT.cqlType)
      def write(r:V:: HNil, data:SettableByIndexData[_], protocolVersion: ProtocolVersion):Unit =
        writeAt(r,0,data,protocolVersion)

      def writeAt(r:V:: HNil, idx:Int, data:SettableByIndexData[_], protocolVersion: ProtocolVersion):Unit =
        { data.setBytesUnsafe(idx,CT.serialize(r.head,protocolVersion)); () }

      def readAt(idx:Int, data:GettableByIndexData, protocolVersion: ProtocolVersion):Either[Throwable,V :: HNil] =
        CT.deserialize(data.getBytesUnsafe(idx), protocolVersion).right.map(_ :: HNil)

      def read(data:GettableByIndexData, protocolVersion: ProtocolVersion):Either[Throwable,V:: HNil] =
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
      def writeAt(r: ::[V, L], idx: Int, data: SettableByIndexData[_], protocolVersion: ProtocolVersion): Unit =
      { data.setBytesUnsafe(idx,CT.serialize(r.head, protocolVersion)) ; tail.writeAt(r.tail, idx + 1, data, protocolVersion) }

      def write(r: ::[V, L], data: SettableByIndexData[_], protocolVersion: ProtocolVersion): Unit = writeAt(r,0,data,protocolVersion)

      def read(data: GettableByIndexData, protocolVersion: ProtocolVersion): Either[Throwable, ::[V, L]] = readAt(0,data,protocolVersion)
      def readAt(idx: Int, data: GettableByIndexData, protocolVersion: ProtocolVersion): Either[Throwable, ::[V, L]] =
        tail.readAt(idx+1,data,protocolVersion).right.flatMap { t =>
          CT.deserialize(data.getBytesUnsafe(idx), protocolVersion).right.map(_ :: t)
        }

    }

  }

}