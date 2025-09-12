package spinoco.fs2.cassandra.ctype

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.codec.{TypeCodec, TypeCodecs}
import com.datastax.oss.driver.api.core.`type`.{DataType, DataTypes}
import com.datastax.oss.driver.api.core.data.{GettableByName, SettableByName}
import com.datastax.oss.driver.internal.core.`type`.codec.StringCodec
import com.datastax.oss.driver.shaded.guava.common.base.Charsets
import fs2.Chunk
import scodec.bits.BitVector
import scodec.{Attempt, Codec, DecodeResult, SizeBound}
import shapeless.tag.@@
import shapeless.{::, HList, HNil, tag}
import spinoco.fs2.cassandra.baseutil
import spinoco.fs2.cassandra.baseutil.GettableSyntax.{BitVectorReadAsSyntax, GettableByNameSyntax}
import spinoco.fs2.cassandra.baseutil.SettableSyntax.SettableWriteSyntax
import spinoco.fs2.cassandra.ctype.CType.{Ascii, Type1}
import spinoco.fs2.cassandra.ctype.types._

import java.net.{InetAddress, URI}
import java.nio.ByteBuffer
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.{Date, UUID}
import scala.concurrent.duration._
import scala.reflect.ClassTag

/**
  * Used to serialize/deserialize values to c*
  */
trait CType[A] { self =>

  /** C* typecodec instance **/
  def cqlType: DataType

  /**
    * Codec that is used to encode this type instance to cql protocol bytes (not the string representation)
    * @param protocolVersion Version of the protocol for cassandra to use
    * @return
    */
  def cqlCodec(protocolVersion: ProtocolVersion): Codec[A]

  /** parse supplied string **/
  def parse(cql: String): Attempt[A]

  /** serializes the value to be used in CQL statement **/
  def format(a: A): Attempt[String]

  def writeCql(k: String, v: A): Map[String, String] = self.writeFormatted(k, v)
  def writeRaw(k: String, v: A, protocolVersion: ProtocolVersion): Map[String, ByteBuffer] = self.writeRawSerialized(k, v, protocolVersion)
  def writeByName[D <: SettableByName[D]](k: String, v: A, data: D, protocolVersion: ProtocolVersion): D =  self.writeByNameSerialized(k, v, data, protocolVersion)
  def readByName(k: String, data: GettableByName, protocolVersion: ProtocolVersion): Either[Throwable, A] = data.getBitsByName(k).readAs[A](k, protocolVersion)(self)
  def readByNameIfExists(keys: Set[String], k: String, data: GettableByName, protocolVersion: ProtocolVersion): Either[Throwable, A] = {
    if (keys.contains(k.toLowerCase)) {
      readByName(k, data, protocolVersion)
    } else {
      Right(None.asInstanceOf[A])
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

  @inline def apply[A](implicit instance: CType[A]): CType[A] = instance


  implicit class CTypeSyntax[A](val self: CType[A]) extends AnyVal {
    /** create new CType by applying fa and fb to `A` and `B` respectively */
    def xmap[B](fa: A => B, fb: B => A):CType[B] = {
      new CType[B] {
        def cqlType: DataType = self.cqlType
        def cqlCodec(protocolVersion: ProtocolVersion): Codec[B] = self.cqlCodec(protocolVersion).xmap(fa,fb)
        def parse(cql: String): Attempt[B] = self.parse(cql).map(fa)
        def format(a: B): Attempt[String] = self.format(fb(a))
      }
    }

    /** like `xmap` but allows eventually to fail parse `A` to `B` and `B` to `A` **/
    def exmap[B](fa: A => Attempt[B])(fb: B => Attempt[A]):CType[B] = {
      new CType[B] {
        def cqlType: DataType = self.cqlType
        def cqlCodec(protocolVersion: ProtocolVersion): Codec[B] = self.cqlCodec(protocolVersion).exmapc(fa)(fb)
        def parse(cql: String): Attempt[B] = self.parse(cql).flatMap(fa)
        def format(a: B): Attempt[String] = fb(a).flatMap(self.format)
      }
    }
  }

  def fromCodec[A](codec: TypeCodec[A]): CType[A] = {
    new CType[A] {
      def cqlType: DataType = codec.getCqlType

      def cqlCodec(protocolVersion: ProtocolVersion): Codec[A] = new Codec[A] {
        def encode(value: A): Attempt[BitVector] =
          baseutil.attempt(BitVector.view(codec.encode(value, protocolVersion)))

        def sizeBound: SizeBound = SizeBound.unknown

        def decode(bits: BitVector): Attempt[DecodeResult[A]] =
          baseutil.attempt(codec.decode(bits.toByteBuffer, protocolVersion))
          .map(DecodeResult(_, BitVector.empty)) // this is ok hence primitive codec in cassandra must get only that much bytes how much it can consume
      }

      def parse(cql: String): Attempt[A] = baseutil.attempt(codec.parse(cql))
      def format(a: A): Attempt[String] = baseutil.attempt(codec.format(a))
    }
  }

  implicit val stringInstance : CType[String] =
    StringCType.instance(new StringCodec(DataTypes.TEXT, Charsets.UTF_8), Charsets.UTF_8)

  implicit val asciiInstance :CType[String @@ Ascii] =
    StringCType.instance(new StringCodec(DataTypes.ASCII, Charsets.US_ASCII), Charsets.US_ASCII)
    .xmap(tag[Ascii](_), identity)

  implicit val booleanInstance: CType[Boolean] =
    CType.fromCodec(TypeCodecs.BOOLEAN).xmap(j => j,s => s)

  implicit val intInstance : CType[Int] =
    IntCType.instance

  implicit val counterInstance : CType[Long @@ Counter] =
    BigIntCType.instance(DataTypes.COUNTER)
    .xmap(j => tag[Counter](j), s => s)

  implicit val longInstance: CType[Long] =
    BigIntCType.instance(DataTypes.BIGINT)

  implicit val floatInstance:CType[Float] =
    FloatCType.instance

  implicit val doubleInstance:CType[Double] =
    CType.fromCodec(TypeCodecs.DOUBLE).xmap(j => j,s => s)

  implicit val bigDecimalInstance:CType[BigDecimal] =
   CType.fromCodec(TypeCodecs.DECIMAL).xmap(BigDecimal(_), _.bigDecimal)

  implicit val bigIntInstance:CType[BigInt] =
   CType.fromCodec(TypeCodecs.VARINT).xmap(BigInt(_), _.bigInteger)


  implicit val byteBufferInstance: CType[ByteBuffer] =
   CType.fromCodec(TypeCodecs.BLOB)

  implicit val bytesInstance:CType[Chunk[Byte]] =
    byteBufferInstance.xmap(Chunk.byteBuffer,_.toByteBuffer)

  implicit val uuidInstance: CType[UUID]  =  CType.fromCodec(TypeCodecs.UUID)

  implicit val type1UuidInstance: CType[UUID @@ Type1] =
    CType.fromCodec(TypeCodecs.TIMEUUID)
    .xmap(tag[Type1](_), identity)

  implicit val instantInstance: CType[Instant] =
    CType.fromCodec(TypeCodecs.TIMESTAMP)

  implicit val dateInstance:CType[Date] =
    instantInstance.xmap(Date.from, _.toInstant)

  implicit val localDateTimeInstance: CType[LocalDateTime] =
    dateInstance.xmap(
      dt => LocalDateTime.ofInstant(dt.toInstant, ZoneId.systemDefault())
      , ldt => Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant)
    )

  implicit val durationInstance: CType[FiniteDuration] =
    longInstance.xmap(_.millis,_.toMillis)

  implicit val ttlDurationInstance: CType[FiniteDuration @@ TTL] =
    intInstance.xmap(s => tag[TTL](s.seconds),_.toSeconds.toInt)

  implicit val inetAddressInstance:CType[InetAddress] =
    CType.fromCodec(TypeCodecs.INET)

  implicit val uriInstance:CType[URI] =
    stringInstance.exmap(
      s => baseutil.attempt(URI.create(s))
    )(
      uri => Attempt.successful(uri.toString)
    )


  implicit def enumInstance[E <: Enumeration : ClassTag]:CType[E#Value] = {
    lazy val e = implicitly[ClassTag[E]].runtimeClass.getField("MODULE$").get((): Unit).asInstanceOf[Enumeration]
    stringInstance.exmap(
      s => baseutil.attempt(e.withName(s).asInstanceOf[E#Value])
    )(
      e => Attempt.successful(e.toString)
    )
  }

  implicit def optionInstance[A : CType]: CType[Option[A]] =
     OptionCType.instance[A]

  implicit def collectionInstance[C[_] : CollectionType, A : CType]: CType[C[A]] =
    CollectionCType.instance[C, A](dimension = None)

  implicit def mapInstance[K : MapKeyCType, V : CType]:CType[Map[K,V]] =
    MapCType.instance[K,V]



  import shapeless.syntax.std.tuple._

  implicit def tuple2Instance[A,B](implicit hinstance: CType[A :: B :: HNil]):CType[(A,B)] =
    hinstance.xmap(_.tupled,_.productElements)
  implicit def tuple3Instance[A,B,C](implicit hinstance: CType[A :: B :: C :: HNil]):CType[(A,B,C)] =
    hinstance.xmap(_.tupled,_.productElements)
  implicit def tuple4Instance[A,B,C,D](implicit hinstance: CType[A :: B :: C :: D :: HNil]):CType[(A,B,C,D)] =
    hinstance.xmap(_.tupled,_.productElements)
  implicit def tuple5Instance[A,B,C,D,E](implicit hinstance: CType[A :: B :: C :: D :: E:: HNil]):CType[(A,B,C,D,E)] =
    hinstance.xmap(_.tupled,_.productElements)

  implicit def hListInstance[L <: HList : HListCType ]:CType[L] =
     HListCType.instance[L]


}

/** helper to deserialize collections **/
trait CollectionType[C[_]] {
  def zero[A] : C[A]
  def append[A](f:C[A], a:A):C[A]
  def map[A,B](f:C[A], fm: A => B):C[B]
  def cqlType(el:DataType):DataType
  def sizeOf[A](c: C[A]): Int

  /** provides head element and tail if the collection is nonempty */
  def uncons1[A](s: C[A]): Option[(A, C[A])]
}

object CollectionType {

  @inline def apply[C[_]](implicit instance: CollectionType[C]): CollectionType[C] = instance

  implicit val listInstance: CollectionType[List] = new CollectionType[List] {
    def zero[A]: List[A] = List.empty
    def append[A](f: List[A], a: A): List[A] = f :+ a
    def map[A,B](f: List[A], fm: A => B):List[B] = f map fm
    def cqlType(el: DataType): DataType = DataTypes.listOf(el)
    def sizeOf[A](c: List[A]): Int = c.length
    def uncons1[A](s: List[A]): Option[(A, List[A])] = s.headOption.map { h => (h, s.tail)}
  }

  implicit val vectorInstance: CollectionType[Vector] = new CollectionType[Vector] {
    def zero[A]: Vector[A] = Vector.empty
    def append[A](f: Vector[A], a: A): Vector[A] = f :+ a
    def map[A,B](f: Vector[A], fm: A => B):Vector[B] = f map fm
    def cqlType(el: DataType): DataType = DataTypes.listOf(el)
    def sizeOf[A](c: Vector[A]): Int = c.length
    def uncons1[A](s: Vector[A]): Option[(A, Vector[A])] = s.headOption.map { h => (h, s.tail) }
  }

  implicit val setInstance: CollectionType[Set] = new CollectionType[Set] {
    def zero[A]: Set[A] = Set.empty
    def append[A](f: Set[A], a: A): Set[A] = f + a
    def map[A,B](f: Set[A], fm: A => B):Set[B] = f map fm
    def cqlType(el: DataType): DataType = DataTypes.setOf(el)
    def sizeOf[A](c: Set[A]): Int = c.size
    def uncons1[A](s: Set[A]): Option[(A, Set[A])] = s.headOption.map { h => (h, s.tail) }
  }

  implicit val seqInstance: CollectionType[Seq] = new CollectionType[Seq] {
    def zero[A]: Seq[A] = Seq.empty
    def append[A](f: Seq[A], a: A): Seq[A] = f :+ a
    def map[A,B](f: Seq[A], fm: A => B):Seq[B] = f map fm
    def cqlType(el: DataType): DataType = DataTypes.listOf(el)
    def sizeOf[A](c: Seq[A]): Int = c.length
    def foreach[A](c: Seq[A])(f: A => Unit): Unit = c foreach f
    def uncons1[A](s: Seq[A]): Option[(A, Seq[A])] = s.headOption.map { h => (h, s.tail) }
  }



}


trait MapKeyCType[A] extends CType[A]


object MapKeyCType {

  @inline def apply[A](implicit instance: MapKeyCType[A]): MapKeyCType[A] = instance

  implicit class MapKeyCTypeSyntax[A](val self: MapKeyCType[A]) extends AnyVal {
    /** create new CType by applying fa and fb to `A` and `B` respectively */
    def xmap[B](fa: A => B, fb: B => A):MapKeyCType[B] = {
      new MapKeyCType[B] {
        def cqlType: DataType = self.cqlType
        def cqlCodec(protocolVersion: ProtocolVersion): Codec[B] = self.cqlCodec(protocolVersion).xmap(fa,fb)
        def parse(cql: String): Attempt[B] = self.parse(cql).map(fa)
        def format(a: B): Attempt[String] = self.format(fb(a))
      }
    }

    /** like `xmap` but allows eventually to fail parse `A` to `B` and `B` to `A` **/
    def exmap[B](fa: A => Attempt[B])(fb: B => Attempt[A]):MapKeyCType[B] = {
      new MapKeyCType[B] {
        def cqlType: DataType = self.cqlType
        def cqlCodec(protocolVersion: ProtocolVersion): Codec[B] = self.cqlCodec(protocolVersion).exmapc(fa)(fb)
        def parse(cql: String): Attempt[B] = self.parse(cql).flatMap(fa)
        def format(a: B): Attempt[String] = fb(a).flatMap(self.format)
      }
    }
  }


  def fromCType[A](ct: CType[A]): MapKeyCType[A] = {
    new MapKeyCType[A] {
      def cqlType: DataType =
        ct.cqlType

      def cqlCodec(version: ProtocolVersion): Codec[A] =
        ct.cqlCodec(version)

      def parse(cql: String): Attempt[A] =
        ct.parse(cql)

      def format(a: A): Attempt[String] =
        ct.format(a)
    }
  }

  implicit lazy val stringInstance : MapKeyCType[String] = MapKeyCType.fromCType(CType.stringInstance)
  implicit lazy val asciiInstance :MapKeyCType[String @@ Ascii] = MapKeyCType.fromCType(CType.asciiInstance)
  implicit lazy val booleanInstance: MapKeyCType[Boolean] = MapKeyCType.fromCType(CType.booleanInstance)
  implicit lazy val intInstance : MapKeyCType[Int] = MapKeyCType.fromCType(CType.intInstance)
  implicit lazy val longInstance: CType[Long] =  MapKeyCType.fromCType(CType.longInstance)
  implicit lazy val floatInstance: CType[Float] = MapKeyCType.fromCType(CType.floatInstance)
  implicit lazy val doubleInstance: CType[Double] = MapKeyCType.fromCType(CType.doubleInstance)
  implicit lazy val bigDecimalInstance: CType[BigDecimal] =  MapKeyCType.fromCType(CType.bigDecimalInstance)
  implicit lazy val bigIntInstance: CType[BigInt] = MapKeyCType.fromCType(CType.bigIntInstance)
  implicit lazy val uuidInstance: CType[UUID] = MapKeyCType.fromCType(CType.uuidInstance)
  implicit lazy val type1UuidInstance: CType[UUID @@ Type1] = MapKeyCType.fromCType(CType.type1UuidInstance)


}