package spinoco.fs2.cassandra

import java.net.{InetAddress, URI}
import java.nio.ByteBuffer
import java.time.{LocalDateTime, ZoneId}
import java.util.{Date, UUID}

import com.datastax.driver.core._
import fs2.Chunk
import shapeless.{::, HList, HNil, tag}
import shapeless.syntax.std.tuple._
import shapeless.tag.@@
import spinoco.fs2.cassandra.CType.Ascii
import spinoco.fs2.cassandra.internal.CTypeNonEmptyHListInstance

import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

/**
  * Used to serialize/deserialize values to c*
  */
trait CType[A] { self =>

  /** C* typecodec instance **/
  def cqlType: DataType

  /** Serializes this to bytes **/
  def serialize(a:A, protocolVersion: ProtocolVersion):ByteBuffer

  /** Deserialize from bytes, and if successful, passes value on Right **/
  def deserialize(from:ByteBuffer, protocolVersion: ProtocolVersion):Either[Throwable, A]

  /** parse supplied string and if successful, passes value on Right **/
  def parse(from:String):Either[Throwable, A]

  /** serializes the value to be used in CQL statement **/
  def format(a:A):String

  /** create new CType by applying fa and fb to `A` and `B` respectively */
  def map[B](fa: A => B, fb: B => A):CType[B] = {
    new CType[B] {
      def cqlType: DataType = self.cqlType
      def serialize(a: B, protocolVersion: ProtocolVersion): ByteBuffer = self.serialize(fb(a), protocolVersion)
      def parse(from: String): Either[Throwable, B] = self.parse(from).right.map(fa)
      def format(a: B): String = self.format(fb(a))
      def deserialize(from: ByteBuffer, protocolVersion: ProtocolVersion): Either[Throwable, B] = self.deserialize(from,protocolVersion).right.map(fa)
    }
  }

  /** like `map` but allows eventually to fail parse `A` to `B` **/
  def map2[B](fa: A => Either[Throwable, B], fb: B => A):CType[B] = {
    new CType[B] {
      def cqlType: DataType = self.cqlType
      def serialize(a: B, protocolVersion: ProtocolVersion): ByteBuffer = self.serialize(fb(a), protocolVersion)
      def parse(from: String): Either[Throwable, B] = self.parse(from).right.flatMap(fa)
      def format(a: B): String = self.format(fb(a))
      def deserialize(from: ByteBuffer, protocolVersion: ProtocolVersion): Either[Throwable, B] = self.deserialize(from,protocolVersion).right.flatMap(fa)
    }
  }


}


object CType {

  /** marker trait annotating type1 time based uuid **/
  sealed trait Type1

  /** marker trait indicating String to be treated as `ascii` **/
  sealed trait Ascii


  /** marker trait for Long values that acts like CQL counter. Counter must be of long type **/
  sealed trait Counter

  /** marker trait for TTL of the column **/
  sealed trait TTL



  def fromCodec[A](tc:TypeCodec[A]):CType[A] = {
    new CType[A] {
      def cqlType: DataType = tc.getCqlType
      def serialize(a: A, protocolVersion: ProtocolVersion): ByteBuffer = tc.serialize(a,protocolVersion)
      def parse(from: String): Either[Throwable, A] = util.Try(tc.parse(from))
      def format(a: A): String = tc.format(a)
      def deserialize(from: ByteBuffer, protocolVersion: ProtocolVersion): Either[Throwable, A] = util.Try(tc.deserialize(from,protocolVersion))
    }
  }

  def toTypeCodec[A](ct:CType[A])(implicit C:ClassTag[A]):TypeCodec[A] = {
    if (ct.cqlType == DataType.cboolean()) TypeCodec.cboolean().asInstanceOf[TypeCodec[A]]
    else if (ct.cqlType == DataType.cint()) TypeCodec.cint().asInstanceOf[TypeCodec[A]]
    else if (ct.cqlType == DataType.bigint()) TypeCodec.bigint().asInstanceOf[TypeCodec[A]]
    else if (ct.cqlType == DataType.cfloat()) TypeCodec.cfloat().asInstanceOf[TypeCodec[A]]
    else if (ct.cqlType == DataType.cdouble()) TypeCodec.cfloat().asInstanceOf[TypeCodec[A]]
    else {

      new TypeCodec[A](ct.cqlType, C.runtimeClass.asInstanceOf[Class[A]]) {
        def serialize(value: A, protocolVersion: ProtocolVersion): ByteBuffer = ct.serialize(value, protocolVersion)
        def parse(value: String): A = ct.parse(value).fold(throw _, identity)
        def format(value: A): String = ct.format(value)
        def deserialize(bytes: ByteBuffer, protocolVersion: ProtocolVersion): A = ct.deserialize(bytes, protocolVersion).fold(throw _, identity)
      }
    }
  }




  implicit val stringInstance : CType[String] =
    CType.fromCodec(TypeCodec.varchar())

  implicit val asciiInstance :CType[String @@ Ascii] =
    CType.fromCodec(TypeCodec.ascii()).map(tag[Ascii](_), identity)


  implicit val booleanInstance: CType[Boolean] =
    CType.fromCodec(TypeCodec.cboolean()).map(j => j, s => s)

  implicit val intInstance : CType[Int] =
    CType.fromCodec(TypeCodec.cint()).map(j => j, s => s)

  implicit val counterInstance : CType[Long @@ Counter] =
    CType.fromCodec(TypeCodec.counter()).map(j => tag[Counter](j), s => s)

  implicit val longInstance: CType[Long] =
    CType.fromCodec(TypeCodec.bigint()) .map(j => j, s => s)

  implicit val floatInstance:CType[Float] =
    CType.fromCodec(TypeCodec.cfloat()) .map(j => j,s => s)

  implicit val doubleInstance:CType[Double] =
    CType.fromCodec(TypeCodec.cdouble()) .map(j => j,s => s)

  implicit val bigDecimalInstance:CType[BigDecimal] =
   CType.fromCodec(TypeCodec.decimal()).map(BigDecimal(_), _.bigDecimal)

  implicit val bigIntInstance:CType[BigInt] =
   CType.fromCodec(TypeCodec.varint()).map(BigInt(_), _.bigInteger)


  implicit val byteBufferInstance: CType[ByteBuffer] =
   CType.fromCodec(TypeCodec.blob())

  implicit val bytesInstance:CType[Chunk[Byte]] =
    byteBufferInstance.map(
      { bb =>  val bb0 = bb.duplicate(); val arr = Array.ofDim[Byte](bb.remaining); bb.get(arr); Chunk.bytes(arr) } // todo likely we don't have to copy here
      , { bs => val bs0 = bs.toBytes; ByteBuffer.wrap(bs0.values, bs0.offset, bs0.size)
      }
    )




  implicit val uuidInstance: CType[UUID]  =  CType.fromCodec(TypeCodec.uuid())
  implicit val type1UuidInstance: CType[UUID @@ Type1] =
    CType.fromCodec(TypeCodec.timeUUID())
    .map(tag[Type1](_), identity)

  implicit val dateInstance:CType[Date] = CType.fromCodec(TypeCodec.timestamp())

  implicit val localDateTimeInstance: CType[LocalDateTime] =
    dateInstance.map(
      dt => LocalDateTime.ofInstant(dt.toInstant, ZoneId.systemDefault())
      , ldt => Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant)
    )

  implicit val durationInstance: CType[FiniteDuration] =
    longInstance.map(_.millis,_.toMillis)

  implicit val ttlDurationInstance: CType[FiniteDuration @@ TTL] =
    intInstance.map(s => tag[TTL](s.seconds),_.toSeconds.toInt)


  implicit val inetAddressInstance:CType[InetAddress] =
    CType.fromCodec(TypeCodec.inet())

  implicit val uriInstance:CType[URI] =
    stringInstance.map2 (
      s => util.Try(URI.create(s))
      , _.toString
    )


  implicit def enumInstance[E <: Enumeration : ClassTag]:CType[E#Value] = {
    lazy val e = implicitly[ClassTag[E]].runtimeClass.getField("MODULE$").get((): Unit).asInstanceOf[Enumeration]
    stringInstance.map2(
      { s => util.Try(e.withName(s).asInstanceOf[E#Value]) }
      , _.toString
    )
  }

  implicit def optionInstance[A : ClassTag](implicit CT: CType[A]):CType[Option[A]] = {
     new CType[Option[A]] {
       def cqlType: DataType = CT.cqlType
       def serialize(a: Option[A], protocolVersion: ProtocolVersion): ByteBuffer = {
         a match {
           case None => null
           case Some(a) => CT.serialize(a,protocolVersion)
         }
       }
       def parse(from: String): Either[Throwable, Option[A]] = {
         if (from == null || from.toUpperCase == "NULL") Right(None)
         else CT.parse(from).right.map(Some(_))
       }
       def format(a: Option[A]): String = a match {
         case None => "NULL"
         case Some(a) => CT.format(a)
       }
       def deserialize(from: ByteBuffer, protocolVersion: ProtocolVersion): Either[Throwable, Option[A]] = {
         if (from == null) Right(None)
         else CT.deserialize(from,protocolVersion).right.map(Some(_))
       }
     }
  }

  implicit def listInstance[A:ClassTag](implicit CT:CType[A]):CType[List[A]] =
    fromCodec(TypeCodec.list(toTypeCodec(CT))).map(_.asScala.toList,_.asJava)

  implicit def vectorInstance[A](implicit CT:CType[A], C:ClassTag[A]):CType[Vector[A]] =
    fromCodec(TypeCodec.list(toTypeCodec(CT))).map(_.asScala.toVector,_.asJava)

  implicit def seqInstance[A](implicit CT:CType[A], C:ClassTag[A]):CType[Seq[A]] =
    fromCodec(TypeCodec.list(toTypeCodec(CT))).map(_.asScala.toSeq,_.asJava)

  implicit def setInstance[A:ClassTag](implicit CT:CType[A]):CType[Set[A]] =
    fromCodec(TypeCodec.set(toTypeCodec(CT))).map(_.asScala.toSet,_.asJava)



  implicit def tuple2Instance[A,B](implicit hinstance: CType[A :: B :: HNil]):CType[(A,B)] =
    hinstance.map(_.tupled,_.productElements)
  implicit def tuple3Instance[A,B,C](implicit hinstance: CType[A :: B :: C :: HNil]):CType[(A,B,C)] =
    hinstance.map(_.tupled,_.productElements)
  implicit def tuple4Instance[A,B,C,D](implicit hinstance: CType[A :: B :: C :: D :: HNil]):CType[(A,B,C,D)] =
    hinstance.map(_.tupled,_.productElements)
  implicit def tuple5Instance[A,B,C,D,E](implicit hinstance: CType[A :: B :: C :: D :: E:: HNil]):CType[(A,B,C,D,E)] =
    hinstance.map(_.tupled,_.productElements)

  implicit def hListInstance[L <: HList](
    implicit
    CT:CTypeNonEmptyHListInstance[L]
    , clz: ClassTag[L]
  ):CType[L] = {
    val tt = TupleType.of(ProtocolVersion.V3, CodecRegistry.DEFAULT_INSTANCE, CT.types:_*)
    val tc = TypeCodec.tuple(tt)

    def toTupleValue(l:L, protocolVersion: ProtocolVersion):TupleValue = {
      val tv = tt.newValue()
      CT.write(l, tv, protocolVersion)
      tv
    }

     new CType[L] {
        def cqlType: DataType = tt
        def serialize(a: L, protocolVersion: ProtocolVersion): ByteBuffer = tc.serialize(toTupleValue(a,protocolVersion), protocolVersion)
        def parse(from: String): Either[Throwable, L] = util.Try(tc.parse(from)).right.flatMap(tv => CT.read(tv, ProtocolVersion.V3))
        def format(a: L): String = tc.format(toTupleValue(a, ProtocolVersion.V3))
        def deserialize(from: ByteBuffer, protocolVersion: ProtocolVersion): Either[Throwable, L] =
          util.Try(tc.deserialize(from,protocolVersion)).right.flatMap(tv => CT.read(tv, protocolVersion))
     }


  }


  implicit def stringMapInstance[V:ClassTag]( implicit VT: CType[V] ):CType[Map[String,V]] =
    fromCodec(TypeCodec.map(TypeCodec.varchar(),toTypeCodec(VT)))
    .map( _.asScala.toMap , _.asJava )

  implicit def intMapInstance[V:ClassTag]( implicit VT: CType[V] ):CType[Map[Int,V]] =
    fromCodec(TypeCodec.map(TypeCodec.cint(),toTypeCodec(VT)))
      .map(
        _.asScala.toMap.map{ case(i,v) => i.toInt -> v }
        ,_.map{ case(i,v) => Integer.valueOf(i) -> v }.asJava
      )

  implicit def longMapInstance[V:ClassTag]( implicit VT: CType[V] ):CType[Map[Long,V]] =
    fromCodec(TypeCodec.map(TypeCodec.bigint(),toTypeCodec(VT)))
      .map(
        _.asScala.toMap.map{ case(l,v) => l.toLong -> v }
        ,_.map{ case(l,v) => java.lang.Long.valueOf(l) -> v }.asJava
      )



}


trait MapKeyCType[A] extends CType[A]


object MapKeyCType {

  def apply[A](ct:CType[A]):MapKeyCType[A] = {
    new MapKeyCType[A] {
      def cqlType: DataType = ct.cqlType
      def serialize(a: A, protocolVersion: ProtocolVersion): ByteBuffer = ct.serialize(a,protocolVersion)
      def parse(from: String): Either[Throwable, A] = ct.parse(from)
      def format(a: A): String = ct.format(a)
      def deserialize(from: ByteBuffer, protocolVersion: ProtocolVersion): Either[Throwable, A] = ct.deserialize(from,protocolVersion)
    }
  }

  implicit lazy val stringInstance : MapKeyCType[String] = MapKeyCType(CType.stringInstance)
  implicit lazy val asciiInstance :MapKeyCType[String @@ Ascii] = MapKeyCType(CType.asciiInstance)
  implicit lazy val booleanInstance: MapKeyCType[Boolean] = MapKeyCType(CType.booleanInstance)
  implicit lazy val intInstance : MapKeyCType[Int] = MapKeyCType(CType.intInstance)
  implicit lazy val longInstance: CType[Long] =  MapKeyCType(CType.longInstance)
  implicit lazy val floatInstance:CType[Float] = MapKeyCType(CType.floatInstance)
  implicit lazy val doubleInstance:CType[Double] = MapKeyCType(CType.doubleInstance)
  implicit lazy val bigDecimalInstance:CType[BigDecimal] =  MapKeyCType(CType.bigDecimalInstance)
  implicit lazy val bigIntInstance:CType[BigInt] = MapKeyCType(CType.bigIntInstance)


}