package spinoco.fs2.cassandra.internal

import java.nio.ByteBuffer

import com.datastax.driver.core._
import shapeless.labelled._
import shapeless.{::, HList, HNil, Witness}
import spinoco.fs2.cassandra.CType
import spinoco.fs2.cassandra.util

/**
  * Created by pach on 04/06/16.
  */
trait CTypeNonEmptyRecordInstance[R <: HList] extends CTypeRecordInstance[R]


object CTypeNonEmptyRecordInstance {

  type Aux[R<: HList, CT <: HList] = CTypeNonEmptyRecordInstance[R] { type CTypes = CT }


  implicit def tailInstance[K,V](
    implicit
    tpe: CType[V]
    , wt: Witness.Aux[K]
  ):CTypeNonEmptyRecordInstance.Aux[FieldType[K,V] :: HNil, FieldType[K,CType[V]] :: HNil] =
    new CTypeNonEmptyRecordInstance[FieldType[K,V] :: HNil] {
      type CTypes = FieldType[K,CType[V]] :: HNil
      val k = keyOf(wt)
      val types = Seq(k -> tpe.cqlType)
      def readAt(index:Int, data: GettableByIndexData, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], HNil]] =
        tpe.deserialize(data.getBytesUnsafe(index), protocolVersion).right.map(v => field[K](v) :: HNil)
      def read(data: GettableByIndexData, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], HNil]] =
        readAt(0,data,protocolVersion)
      def readByName(data: GettableByNameData, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], HNil]] = {
        tpe.deserialize(data.getBytesUnsafe(k), protocolVersion).right.map(v => field[K](v) :: HNil)
      }
      def readByNameIfExists(keys: Set[String], data: GettableByNameData, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], HNil]] = {
        (for {
          bytes <- (if (keys.contains(k.toLowerCase)) util.Try(data.getBytesUnsafe(k)) else Right(null)).right
          read <- tpe.deserialize(bytes,protocolVersion).right
        } yield field[K](read) :: HNil
          ).left.map(err => new Throwable(s"Failed to read $k from GettableByNameData (protocol:$protocolVersion), if exists", err))
      }

      def writeCql(r: ::[FieldType[K, V], HNil]): Map[String, String] =
        Map(k -> tpe.format(r.head))
      def writeRaw(r: ::[FieldType[K, V], HNil], protocolVersion: ProtocolVersion): Map[String, ByteBuffer] =
        Map(k -> tpe.serialize(r.head, protocolVersion))
      def write(r: ::[FieldType[K, V], HNil], data: SettableByIndexData[_], protocolVersion: ProtocolVersion): Unit =
        writeAt(r,0,data,protocolVersion)
      def writeAt(r: ::[FieldType[K, V], HNil], idx: Int, data: SettableByIndexData[_], protocolVersion: ProtocolVersion): Unit =
        { data.setBytesUnsafe(idx,tpe.serialize(r.head, protocolVersion)); () }
      def writeByName(r: ::[FieldType[K, V], HNil], data: SettableByNameData[_], protocolVersion: ProtocolVersion): Unit =
        { data.setBytesUnsafe(k,tpe.serialize(r.head, protocolVersion)) ; () }
    }

  implicit def instance[K,V,L <: HList, C <: HList](
    implicit
    tail: CTypeNonEmptyRecordInstance.Aux[L,C]
    , tpe: CType[V]
    , wt: Witness.Aux[K]
  ):CTypeNonEmptyRecordInstance.Aux[FieldType[K,V] :: L, FieldType[K,CType[V]] :: C] = {
    new CTypeNonEmptyRecordInstance[FieldType[K,V] :: L] {
      type CTypes = FieldType[K,CType[V]] :: C
      val k = keyOf(wt)
      val types = (k -> tpe.cqlType) +: tail.types



      def readAt(idx:Int, data: GettableByIndexData, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], L]] = {
        (for {
          bytes <- util.Try(data.getBytesUnsafe(idx)).right
          read <- tpe.deserialize(bytes,protocolVersion).right
          tr <- tail.readAt(idx+1,data,protocolVersion).right
        } yield field[K](read) :: tr
          ).left.map(err => new Throwable(s"Failed to read $k from GettableByIndexData (protocol:$protocolVersion)", err))
      }


      def readByName(data: GettableByNameData, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], L]] = {
        (for {
          bytes <- util.Try(data.getBytesUnsafe(k)).right
          read <- tpe.deserialize(bytes,protocolVersion).right
          tr <- tail.readByName(data,protocolVersion).right
        } yield field[K](read) :: tr
          ).left.map(err => new Throwable(s"Failed to read $k from GettableByNameData (protocol:$protocolVersion)", err))
      }


      def readByNameIfExists(keys: Set[String], data: GettableByNameData, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], L]] = {
        (for {
          bytes <- (if (keys.contains(k.toLowerCase)) util.Try(data.getBytesUnsafe(k)) else Right(null)).right
          read <- tpe.deserialize(bytes,protocolVersion).right
          tr <- tail.readByNameIfExists(keys,data,protocolVersion).right
        } yield field[K](read) :: tr
        ).left.map(err => new Throwable(s"Failed to read $k from GettableByNameData (protocol:$protocolVersion), if exists", err))
      }

      def read(data: GettableByIndexData, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], L]] =
        readAt(0,data,protocolVersion)


      def writeCql(r: ::[FieldType[K, V], L]): Map[String, String] =
        Map(k -> tpe.format(r.head)) ++ tail.writeCql(r.tail)

      def writeRaw(r: ::[FieldType[K, V], L], protocolVersion: ProtocolVersion): Map[String, ByteBuffer] =
        Map(k -> tpe.serialize(r.head, protocolVersion)) ++ tail.writeRaw(r.tail, protocolVersion)

      def write(r: ::[FieldType[K, V], L], data: SettableByIndexData[_], protocolVersion: ProtocolVersion): Unit =  writeAt(r,0,data,protocolVersion)
      def writeAt(r: ::[FieldType[K, V], L], idx: Int, data: SettableByIndexData[_], protocolVersion: ProtocolVersion): Unit = {
        data.setBytesUnsafe(idx,tpe.serialize(r.head, protocolVersion))
        tail.writeAt(r.tail,idx+1,data,protocolVersion)
      }

      def writeByName(r: ::[FieldType[K, V], L], data: SettableByNameData[_], protocolVersion: ProtocolVersion): Unit = {
        data.setBytesUnsafe(k,tpe.serialize(r.head, protocolVersion))
        tail.writeByName(r.tail,data,protocolVersion)
      }
    }
  }





}


trait CTypeRecordInstance[R <: HList] {
  type CTypes
  lazy val typeMap:Map[String,DataType] = types.toMap
  def types:Seq[(String,DataType)]
  def writeCql(r:R):Map[String,String]
  def writeRaw(r:R, protocolVersion: ProtocolVersion):Map[String,ByteBuffer]
  def write(r:R, data:SettableByIndexData[_], protocolVersion: ProtocolVersion):Unit
  def writeByName(r:R, data:SettableByNameData[_], protocolVersion: ProtocolVersion):Unit
  def writeAt(r:R, idx:Int, data:SettableByIndexData[_], protocolVersion: ProtocolVersion):Unit
  def readAt(idx:Int, data:GettableByIndexData, protocolVersion: ProtocolVersion):Either[Throwable,R]
  def read(data:GettableByIndexData, protocolVersion: ProtocolVersion):Either[Throwable,R]
  def readByName(data:GettableByNameData, protocolVersion: ProtocolVersion):Either[Throwable,R]
  def readByNameIfExists(keys:Set[String],data:GettableByNameData, protocolVersion: ProtocolVersion):Either[Throwable,R]
}


object CTypeRecordInstance {

  type Aux[R<: HList, CT <: HList] = CTypeRecordInstance[R] { type CTypes = CT }



  implicit val emptyInstance :CTypeRecordInstance.Aux[HNil, HNil] = new CTypeRecordInstance[HNil] {
    type CTypes = HNil
    def writeCql(r: HNil): Map[String, String] = Map.empty
    def writeRaw(r: HNil, protocolVersion: ProtocolVersion): Map[String, ByteBuffer] = Map.empty
    def types: Seq[(String, DataType)] = Nil
    def readAt(idx:Int, data: GettableByIndexData, protocolVersion: ProtocolVersion): Either[Throwable, HNil] = Right(HNil)
    def read(data: GettableByIndexData, protocolVersion: ProtocolVersion): Either[Throwable, HNil] = Right(HNil)
    def readByName(data: GettableByNameData, protocolVersion: ProtocolVersion): Either[Throwable, HNil] = Right(HNil)
    def readByNameIfExists(keys: Set[String], data: GettableByNameData, protocolVersion: ProtocolVersion): Either[Throwable, HNil] = Right(HNil)
    def write(r:HNil, data: SettableByIndexData[_], protocolVersion: ProtocolVersion): Unit = ()
    def writeAt(r:HNil, idx: Int, data: SettableByIndexData[_], protocolVersion: ProtocolVersion): Unit = ()
    def writeByName(r: HNil, data: SettableByNameData[_], protocolVersion: ProtocolVersion): Unit = ()
  }

  implicit def instance[K,V, T <: HList, TC <: HList](
    implicit
    CT: CTypeNonEmptyRecordInstance[FieldType[K,V] :: T]
  ): CTypeRecordInstance.Aux[FieldType[K,V] :: T, FieldType[K,CType[V]] :: TC] = {
    new CTypeRecordInstance[FieldType[K,V] :: T] {
      type CTypes = FieldType[K,CType[V]] :: TC
      def writeCql(r: ::[FieldType[K, V], T]): Map[String, String] = CT.writeCql(r)
      def writeRaw(r: ::[FieldType[K, V], T], protocolVersion: ProtocolVersion): Map[String, ByteBuffer] = CT.writeRaw(r, protocolVersion)
      def write(r: ::[FieldType[K, V], T], data: SettableByIndexData[_], protocolVersion: ProtocolVersion): Unit = CT.write(r,data,protocolVersion)
      def writeAt(r: ::[FieldType[K, V], T], idx: Int, data: SettableByIndexData[_], protocolVersion: ProtocolVersion): Unit = CT.writeAt(r,idx,data,protocolVersion)
      def writeByName(r: ::[FieldType[K, V], T], data: SettableByNameData[_], protocolVersion: ProtocolVersion): Unit = CT.writeByName(r,data,protocolVersion)
      def types: Seq[(String, DataType)] = CT.types
      def readAt(idx:Int, data:GettableByIndexData, protocolVersion: ProtocolVersion):Either[Throwable, ::[FieldType[K, V], T]]  = CT.readAt(idx,data, protocolVersion)
      def read(index: GettableByIndexData, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], T]] = CT.read(index, protocolVersion)
      def readByName(data: GettableByNameData, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], T]] = CT.readByName(data,protocolVersion)
      def readByNameIfExists(keys: Set[String], data: GettableByNameData, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], T]] = CT.readByNameIfExists(keys,data,protocolVersion)
    }
  }
}