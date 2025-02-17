package spinoco.fs2.cassandra.internal

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.data.{GettableByIndex, GettableByName, SettableByIndex, SettableByName}
import shapeless.labelled._
import shapeless.{::, HList, HNil, Witness}
import spinoco.fs2.cassandra.CType
import spinoco.fs2.cassandra.internal.CodecWriter.CodecWriteSyntax
import spinoco.fs2.cassandra.util.AnnotatedException
import spinoco.fs2.cassandra.util.CTypeReader.CTypeReaderSyntax
import spinoco.fs2.cassandra.util.GettableSyntax.{GettableByIndexSyntax, GettableByNameSyntax}

import java.nio.ByteBuffer

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
  ):CTypeNonEmptyRecordInstance.Aux[FieldType[K,V] :: HNil, FieldType[K,CType[V]] :: HNil] = {
    new CTypeNonEmptyRecordInstance[FieldType[K,V] :: HNil] {
      type CTypes = FieldType[K, CType[V]] :: HNil
      val k = keyOf(wt)
      val types = Seq(k -> tpe.cqlType)
      def readAt(index: Int, data: GettableByIndex, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], HNil]] =
        data.getBitsByIndex(index).readAs[K, V, HNil](k, protocolVersion) { Right(HNil) }
      def read(data: GettableByIndex, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], HNil]] =
        readAt(0, data, protocolVersion).left.map(AnnotatedException.withField(_, k))
      def readByName(data: GettableByName, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], HNil]] =
        data.getBitsByName(k).readAs[K, V, HNil](k, protocolVersion) { Right(HNil) }
      def readByNameIfExists(keys: Set[String], data: GettableByName, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], HNil]] =
        if (keys.contains(k.toLowerCase)) {
          readByName(data, protocolVersion)
        } else {
          Right(field[K](None.asInstanceOf[V]) :: HNil)
        }
      def writeCql(r: ::[FieldType[K, V], HNil]): Map[String, String] =
        tpe.format(r.head)
          .toOption.map(v => Map(k -> v))
          .getOrElse(Map.empty)
      def writeRaw(r: ::[FieldType[K, V], HNil], protocolVersion: ProtocolVersion): Map[String, ByteBuffer] = tpe.writeRawSerialized(k, r.head, protocolVersion)
      def write[D <: SettableByIndex[D]](r: ::[FieldType[K, V], HNil], data: D, protocolVersion: ProtocolVersion): D = writeAt(r,0,data,protocolVersion)
      def writeAt[D <: SettableByIndex[D]](r: ::[FieldType[K, V], HNil], idx: Int, data: D, protocolVersion: ProtocolVersion): D = tpe.writeAtSerialized(idx, r.head, data, protocolVersion)
      def writeByName[D <: SettableByName[D]](r: ::[FieldType[K, V], HNil], data: D, protocolVersion: ProtocolVersion): D = tpe.writeByNameSerialized(k, r.head, data, protocolVersion)
    }
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

      def readAt(idx:Int, data: GettableByIndex, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], L]] = {
        data.getBitsByIndex(idx).readAs[K, V, L](k, protocolVersion) { tail.readAt(idx + 1, data, protocolVersion) }
      }

      def readByName(data: GettableByName, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], L]] = {
        data.getBitsByName(k).readAs[K, V, L](k, protocolVersion) { tail.readByName(data, protocolVersion) }
      }

      def readByNameIfExists(keys: Set[String], data: GettableByName, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], L]] = {
        if (keys.contains(k.toLowerCase)) {
          readByName(data, protocolVersion)
        } else {
          tail.readByNameIfExists(keys,data,protocolVersion).right.map { tail =>
            field[K](None.asInstanceOf[V]) :: tail
          }
        }
      }

      def read(data: GettableByIndex, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], L]] =
        readAt(0,data,protocolVersion)

      def writeCql(r: ::[FieldType[K, V], L]): Map[String, String] = {
        val head_ = tpe.writeFormatted(k, r.head)
        val tail_ = tail.writeCql(r.tail)
        head_ ++ tail_
      }

      def writeRaw(r: ::[FieldType[K, V], L], protocolVersion: ProtocolVersion): Map[String, ByteBuffer] = {
        val head_ = tpe.writeRawSerialized(k, r.head, protocolVersion)
        val tail_ = tail.writeRaw(r.tail, protocolVersion)
        head_ ++ tail_
      }

      def write[D <: SettableByIndex[D]](r: ::[FieldType[K, V], L], data: D, protocolVersion: ProtocolVersion): D =  writeAt(r,0,data,protocolVersion)
      def writeAt[D <: SettableByIndex[D]](r: ::[FieldType[K, V], L], idx: Int, data: D, protocolVersion: ProtocolVersion): D = {
        val written = tpe.writeAtSerialized(idx, r.head, data, protocolVersion)
        tail.writeAt(r.tail,idx+1,written,protocolVersion)
      }

      def writeByName[D <: SettableByName[D]](r: ::[FieldType[K, V], L], data: D, protocolVersion: ProtocolVersion): D = {
        val written = tpe.writeByNameSerialized(k, r.head, data, protocolVersion)
        tail.writeByName(r.tail,written,protocolVersion)
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
  def write[D <: SettableByIndex[D]](r:R, data:D, protocolVersion: ProtocolVersion):D
  def writeByName[D <: SettableByName[D]](r:R, data:D, protocolVersion: ProtocolVersion):D
  def writeAt[D <: SettableByIndex[D]](r:R, idx:Int, data:D, protocolVersion: ProtocolVersion):D
  def readAt(idx:Int, data:GettableByIndex, protocolVersion: ProtocolVersion):Either[Throwable,R]
  def read(data:GettableByIndex, protocolVersion: ProtocolVersion):Either[Throwable,R]
  def readByName(data:GettableByName, protocolVersion: ProtocolVersion):Either[Throwable,R]
  def readByNameIfExists(keys:Set[String],data:GettableByName, protocolVersion: ProtocolVersion):Either[Throwable,R]
}


object CTypeRecordInstance {

  type Aux[R<: HList, CT <: HList] = CTypeRecordInstance[R] { type CTypes = CT }

  implicit val emptyInstance :CTypeRecordInstance.Aux[HNil, HNil] = new CTypeRecordInstance[HNil] {
    type CTypes = HNil
    def writeCql(r: HNil): Map[String, String] = Map.empty
    def writeRaw(r: HNil, protocolVersion: ProtocolVersion): Map[String, ByteBuffer] = Map.empty
    def types: Seq[(String, DataType)] = Nil
    def readAt(idx:Int, data: GettableByIndex, protocolVersion: ProtocolVersion): Either[Throwable, HNil] = Right(HNil)
    def read(data: GettableByIndex, protocolVersion: ProtocolVersion): Either[Throwable, HNil] = Right(HNil)
    def readByName(data: GettableByName, protocolVersion: ProtocolVersion): Either[Throwable, HNil] = Right(HNil)
    def readByNameIfExists(keys: Set[String], data: GettableByName, protocolVersion: ProtocolVersion): Either[Throwable, HNil] = Right(HNil)
    def write[D <: SettableByIndex[D]](r:HNil, data: D, protocolVersion: ProtocolVersion): D = data
    def writeAt[D <: SettableByIndex[D]](r:HNil, idx: Int, data: D, protocolVersion: ProtocolVersion): D = data
    def writeByName[D <: SettableByName[D]](r: HNil, data: D, protocolVersion: ProtocolVersion): D = data
  }

  implicit def instance[K,V, T <: HList, TC <: HList](
    implicit
    CT: CTypeNonEmptyRecordInstance[FieldType[K,V] :: T]
  ): CTypeRecordInstance.Aux[FieldType[K,V] :: T, FieldType[K,CType[V]] :: TC] = {
    new CTypeRecordInstance[FieldType[K,V] :: T] {
      type CTypes = FieldType[K,CType[V]] :: TC
      def writeCql(r: ::[FieldType[K, V], T]): Map[String, String] = CT.writeCql(r)
      def writeRaw(r: ::[FieldType[K, V], T], protocolVersion: ProtocolVersion): Map[String, ByteBuffer] = CT.writeRaw(r, protocolVersion)
      def write[D <: SettableByIndex[D]](r: ::[FieldType[K, V], T], data: D, protocolVersion: ProtocolVersion): D = CT.write(r,data,protocolVersion)
      def writeAt[D <: SettableByIndex[D]](r: ::[FieldType[K, V], T], idx: Int, data: D, protocolVersion: ProtocolVersion): D = CT.writeAt(r,idx,data,protocolVersion)
      def writeByName[D <: SettableByName[D]](r: ::[FieldType[K, V], T], data: D, protocolVersion: ProtocolVersion): D = CT.writeByName(r,data,protocolVersion)
      def types: Seq[(String, DataType)] = CT.types
      def readAt(idx:Int, data:GettableByIndex, protocolVersion: ProtocolVersion):Either[Throwable, ::[FieldType[K, V], T]]  = CT.readAt(idx,data, protocolVersion)
      def read(index: GettableByIndex, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], T]] = CT.read(index, protocolVersion)
      def readByName(data: GettableByName, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], T]] = CT.readByName(data,protocolVersion)
      def readByNameIfExists(keys: Set[String], data: GettableByName, protocolVersion: ProtocolVersion): Either[Throwable, ::[FieldType[K, V], T]] = CT.readByNameIfExists(keys,data,protocolVersion)
    }
  }
}