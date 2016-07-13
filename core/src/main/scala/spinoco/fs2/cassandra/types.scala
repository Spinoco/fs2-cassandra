package spinoco.fs2.cassandra

import java.net.{InetAddress, URI}
import java.nio.ByteBuffer
import java.time.{LocalDateTime, ZoneId}
import java.util.{Date, UUID}

import com.datastax.driver.core._
import fs2.Chunk
import shapeless.{::, HList, HNil, tag}
import shapeless.tag.@@
import spinoco.fs2.cassandra.CType.Ascii
import spinoco.fs2.cassandra.internal.CTypeNonEmptyHListInstance

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.reflect.ClassTag

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



  implicit def collectionInstance[C[_],A:ClassTag](implicit CT:CType[A], C:CollectionType[C]):CType[C[A]] = {
    new CType[C[A]] {
      def cqlType: DataType = C.cqlType(CT.cqlType)
      def serialize(a: C[A], protocolVersion: ProtocolVersion): ByteBuffer = {
        val serialized = C.toArray(C.map[A,ByteBuffer](a,el => CT.serialize(el, protocolVersion)))
        CodecUtils.pack(serialized, serialized.length, protocolVersion)
      }
      def deserialize(from: ByteBuffer, protocolVersion: ProtocolVersion): Either[Throwable, C[A]] = {
        if (from == null || from.remaining() == 0) Right(C.zero)
        else {
          @tailrec
          def go(acc: C[A], rem: Int, buff: ByteBuffer): Either[Throwable, C[A]] = { // note buff is mutable
            if (rem == 0) Right(acc)
            else {
              val decoded =
                util.Try(CodecUtils.readValue(buff, protocolVersion)).right.flatMap { bs =>
                  CT.deserialize(bs, protocolVersion)
                }

              decoded match {
                case Left(err) => Left(err)
                case Right(a) => go(C.append(acc,a), rem = rem - 1, buff)
              }
            }
          }

          val input: ByteBuffer = from.duplicate // this is what java driver does, not sure if this is really necessary
          util.Try(CodecUtils.readSize(input, protocolVersion)).right.flatMap { sz =>
            go(C.zero, sz, input)
          }
        }
      }

      def format(a: C[A]): String = C.mkCqlString(C.map(a,CT.format))

      def parse(from: String): Either[Throwable, C[A]] = {
        if (from == null || from.isEmpty || from == "NULL") Right(C.zero)
        else {
          val start = ParseUtils.skipSpaces(from, 0)
          if (from.charAt(start) != C.cqlOpeningChar) {
            Left(new Throwable(s"Invalid char at $start expected ${C.cqlOpeningChar}, got ${from.charAt(start)}"))
          } else {
            @tailrec
            def go(acc:C[A], idx:Int):Either[Throwable, C[A]] = {
              if (idx >= from.length ) Left(new Throwable(s"Missing closing character in CQL : $from"))
              else {
                val start2 = ParseUtils.skipSpaces(from, idx)
                if (from.charAt(start2) == C.cqlClosingChar) Right(acc)
                else {
                  val parseValueResult =
                    for {
                      endOfCql <- util.Try(ParseUtils.skipCQLValue(from, start2)).right
                      parsed <- CT.parse(from.substring(start2,endOfCql)).right
                    } yield (parsed, endOfCql)

                  parseValueResult match {
                    case Left(err) => Left(err)
                    case Right((a,next)) => go(C.append(acc,a), next)

                  }
                }
              }
            }

            go(C.zero,start+1)
          }

        }
      }
    }
  }


  implicit def mapInstance[K,V](implicit KT: MapKeyCType[K], VT:CType[V]):CType[Map[K,V]] = {
    new CType[Map[K, V]] {
      def cqlType: DataType = DataType.map(KT.cqlType,VT.cqlType)
      def serialize(a: Map[K, V], protocolVersion: ProtocolVersion): ByteBuffer = {
        val buff = Array.ofDim[ByteBuffer](a.size * 2)
        a.foldLeft(0){
          case (idx,(k,v)) =>
            buff.update(idx*2, KT.serialize(k,protocolVersion))
            buff.update(idx*2+1, VT.serialize(v, protocolVersion))
            idx + 1
        }
        CodecUtils.pack(buff,a.size,protocolVersion)
      }
      def deserialize(from: ByteBuffer, protocolVersion: ProtocolVersion): Either[Throwable, Map[K, V]] = {
        if (from == null || from.remaining() == 0) Right(Map.empty)
        else {
          @tailrec
          def go(acc: Map[K,V], rem: Int, buff: ByteBuffer): Either[Throwable, Map[K,V]] = { // note buff is mutable
            if (rem == 0) Right(acc)
            else {
              val decoded =
                for {
                  bsK <- util.Try(CodecUtils.readValue(buff, protocolVersion)).right
                  k <- KT.deserialize(bsK, protocolVersion).right
                  bsV <- util.Try(CodecUtils.readValue(buff, protocolVersion)).right
                  v <- VT.deserialize(bsV, protocolVersion).right
                } yield k -> v

              decoded match {
                case Left(err) => Left(err)
                case Right(kv) => go(acc + kv, rem = rem - 1, buff)
              }
            }
          }

          val input: ByteBuffer = from.duplicate // this is what java driver does, not sure if this is really necessary
          util.Try(CodecUtils.readSize(input, protocolVersion)).right.flatMap { sz =>
            go(Map.empty, sz, input)
          }

        }

      }

      def format(a: Map[K, V]): String = {
        a.toSeq
          .map { case (k, v) => s"${KT.format(k)} : ${VT.format(v)}"}
          .mkString("{",",","}")
      }

      def parse(from: String): Either[Throwable, Map[K, V]] = {
        if (from == null || from.isEmpty || from == "NULL") Right(Map.empty)
        else {
          val start = ParseUtils.skipSpaces(from, 0)
          if (from.charAt(start) != '{') {
            Left(new Throwable(s"Invalid char at $start expected {, got ${from.charAt(start)}"))
          } else {
            @tailrec
            def go(acc:Map[K,V], idx:Int):Either[Throwable, Map[K,V]] = {
              if (idx >= from.length ) Left(new Throwable(s"Missing closing character in CQL (}) : $from"))
              else {
                val startOfKey = ParseUtils.skipSpaces(from, idx)
                if (from.charAt(startOfKey) == '}') Right(acc)
                else {
                  val parseValueResult =
                    for {
                      endOfKey <- util.Try(ParseUtils.skipCQLValue(from, startOfKey)).right
                      k <- KT.parse(from.substring(startOfKey,endOfKey)).right
                      startSplit <- Right(ParseUtils.skipSpaces(from, endOfKey)).right
                      _ <-  if (from.charAt(startSplit) != ':') Left(new Throwable(s"Expected : at $startSplit but got ${from.charAt(startSplit)}")).right
                            else Right(()).right
                      startOfValue <- Right(ParseUtils.skipSpaces(from, startSplit + 1)).right
                      endOfValue <- util.Try(ParseUtils.skipCQLValue(from, startOfValue)).right
                      v <- VT.parse(from.substring(startOfValue,endOfValue)).right
                    } yield (k -> v, endOfValue)

                  parseValueResult match {
                    case Left(err) => Left(err)
                    case Right((kv,next)) =>  go(acc + kv, next)
                  }
                }
              }
            }

            go(Map.empty,start+1)
          }

        }
      }

    }
  }



  import shapeless.syntax.std.tuple._

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




}

/** helper to deserialize collections **/
trait CollectionType[F[_]] {
  def zero[A] : F[A]
  def append[A](f:F[A],a:A):F[A]
  def map[A,B](f:F[A], fm: A => B):F[B]
  def cqlType(el:DataType):DataType
  def toArray[A:ClassTag](f:F[A]):Array[A]
  def mkCqlString(f:F[String]):String
  def cqlOpeningChar:Char
  def cqlClosingChar:Char
}

object CollectionType {

  implicit val listInstance:CollectionType[List] = new CollectionType[List] {
    def zero[A]: List[A] = List.empty
    def append[A](f: List[A], a: A): List[A] = f :+ a
    def map[A,B](f: List[A], fm: A => B):List[B] = f map fm
    def cqlType(el: DataType): DataType = DataType.list(el)
    def toArray[A :ClassTag](f:List[A]):Array[A] = f.toArray
    def mkCqlString(f: List[String]): String = f.mkString("[",",","]")
    val cqlOpeningChar:Char = '['
    val cqlClosingChar:Char = ']'
  }

  implicit val vectorInstance:CollectionType[Vector] = new CollectionType[Vector] {
    def zero[A]: Vector[A] = Vector.empty
    def append[A](f: Vector[A], a: A): Vector[A] = f :+ a
    def map[A,B](f: Vector[A], fm: A => B):Vector[B] = f map fm
    def cqlType(el: DataType): DataType = DataType.list(el)
    def toArray[A:ClassTag](f:Vector[A]):Array[A] = f.toArray
    def mkCqlString(f: Vector[String]): String = f.mkString("[",",","]")
    val cqlOpeningChar:Char = '['
    val cqlClosingChar:Char = ']'
  }

  implicit val setInstance:CollectionType[Set] = new CollectionType[Set] {
    def zero[A]: Set[A] = Set.empty
    def append[A](f: Set[A], a: A): Set[A] = f + a
    def map[A,B](f: Set[A], fm: A => B):Set[B] = f map fm
    def cqlType(el: DataType): DataType = DataType.set(el)
    def toArray[A:ClassTag](f:Set[A]):Array[A] = f.toArray
    def mkCqlString(f: Set[String]): String = f.mkString("[",",","]")
    val cqlOpeningChar:Char = '['
    val cqlClosingChar:Char = ']'
  }

  implicit val seqInstance:CollectionType[Seq] = new CollectionType[Seq] {
    def zero[A]: Seq[A] = Seq.empty
    def append[A](f: Seq[A], a: A): Seq[A] = f :+ a
    def map[A,B](f: Seq[A], fm: A => B):Seq[B] = f map fm
    def cqlType(el: DataType): DataType = DataType.list(el)
    def toArray[A:ClassTag](f:Seq[A]):Array[A] = f.toArray
    def mkCqlString(f: Seq[String]): String = f.mkString("[",",","]")
    val cqlOpeningChar:Char = '['
    val cqlClosingChar:Char = ']'
  }



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
  implicit lazy val floatInstance: CType[Float] = MapKeyCType(CType.floatInstance)
  implicit lazy val doubleInstance: CType[Double] = MapKeyCType(CType.doubleInstance)
  implicit lazy val bigDecimalInstance: CType[BigDecimal] =  MapKeyCType(CType.bigDecimalInstance)
  implicit lazy val bigIntInstance: CType[BigInt] = MapKeyCType(CType.bigIntInstance)


}