package spinoco.fs2.cassandra.ctype.types

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.internal.core.`type`.DefaultTupleType
import com.datastax.oss.driver.internal.core.`type`.codec.ParseUtils
import scodec.bits.BitVector
import scodec.{Attempt, Codec, DecodeResult, Err, SizeBound}
import shapeless.{::, HList, HNil}
import spinoco.fs2.cassandra.baseutil
import spinoco.fs2.cassandra.ctype.CType

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

trait HListCType[L <: HList]  {
  // CType of this hlist `L` types
  type CTypes

  /** all  types in this hlist */
  def types:Seq[DataType]


  /**
    * Codec that is used to encode this type instance to cql protocol bytes (not the string representation)
    * @param protocolVersion Version of the protocol for cassandra to use
    * @return
    */
  def cqlCodec(protocolVersion: ProtocolVersion): Codec[L]


  /** parse supplied string segmented by cql values **/
  def parse(cql: Seq[String]): Attempt[L]

  /** serializes the value to be used in CQL statement **/
  def format(value: L): Attempt[Seq[String]]
}


object HListCType {

  // Encoding: each field as a [bytes] value ([bytes] = int length + contents, null is
  // represented by -1)
  // However we don't want to have nulls in scala so on read we are passing empty byte array
  // to leave decision to the item codec how to decode (option, empty...)

  type Aux[R <: HList, CT <: HList] = HListCType[R] { type CTypes = CT }

  @inline def apply[R <: HList](implicit instance: HListCType[R]): HListCType[R] = instance

  /**
    * Matrializes CType for HList from HListCType
    * @tparam L
    * @return
    */
  def instance[L <:  HList : HListCType]: CType[L] = {
    new CType[L] {
      def cqlType: DataType = new DefaultTupleType(HListCType[L].types.asJava)

      def cqlCodec(protocolVersion: ProtocolVersion): Codec[L] =
        HListCType[L].cqlCodec(protocolVersion)

      def parse(cql: String): Attempt[L] = {
        @tailrec
        def go(cql: String, acc: List[String]): Attempt[List[String]] = {
          baseutil.attempt(ParseUtils.skipCQLValue(cql, 0)) match {
            case Attempt.Successful(-1) => Attempt.successful(acc.reverse)
            case Attempt.Successful(n) =>
              val (value, rest) = cql.splitAt(n)
              go(rest.drop(1), value +: acc)
            case Attempt.Failure(err) => Attempt.failure(err)
          }
        }

        go(cql, Nil).flatMap { values =>
          HListCType[L].parse(values)
        }

      }


      def format(a: L): Attempt[String] = {
        HListCType[L].format(a).map(_.mkString("(", ",", ")"))
      }
    }
  }

  /** HList instance for the HNil when Nil is in the tail */
  implicit val hnilInstance: HListCType.Aux[HNil, HNil] = {
    new HListCType[HNil] {
      type CTypes = HNil

      def types: Seq[DataType] = Nil

      def cqlCodec(protocolVersion: ProtocolVersion): Codec[HNil] = {
        new Codec[HNil] {
          def encode(value: HNil): Attempt[BitVector] = Attempt.successful(BitVector.empty)

          def sizeBound: SizeBound = SizeBound.exact(0)

          def decode(bits: BitVector): Attempt[DecodeResult[HNil]] =
            Attempt.successful(DecodeResult(HNil, bits))
        }
      }

      def parse(cql: Seq[String]): Attempt[HNil] = {
        if (cql.isEmpty) Attempt.successful(HNil)
        else Attempt.failure(Err(s"Expected empty list, but got ${cql}"))
      }

      def format(value: HNil): Attempt[Seq[String]] =
        Attempt.successful(Nil)
    }
  }


  /**
    * instance of the HList type on the head position
    */
  implicit def hlistInstance[V : CType, L <: HList : HListCType, C <: HList]: HListCType.Aux[V :: L, CType[V] :: C] = {
    new HListCType[V :: L] {
      type CTypes = CType[V] :: C

      def types: Seq[DataType] = CType[V].cqlType +: HListCType[L].types

      def cqlCodec(protocolVersion: ProtocolVersion): Codec[V :: L] = {
        val elementCodec = codecs.elementCodec(CType[V].cqlCodec(protocolVersion))
        new Codec[V :: L] {
          def encode(value: V :: L): Attempt[BitVector] = {
            elementCodec.encode(value.head).flatMap { headBits =>
            HListCType[L].cqlCodec(protocolVersion).encode(value.tail).map { tailBits =>
              headBits ++ tailBits
            }}
          }

          def sizeBound: SizeBound = SizeBound.atLeast(32)

          def decode(bits: BitVector): Attempt[DecodeResult[V :: L]] = {
            elementCodec.decode(bits).flatMap { case DecodeResult(head, tailBits) =>
            HListCType[L].cqlCodec(protocolVersion).decode(tailBits).map { case DecodeResult(tail, remainingBits) =>
              DecodeResult(head :: tail, remainingBits)
            }}
          }
        }
      }

      def parse(cql: Seq[String]): Attempt[V :: L] = {
        if (cql.size != types.size) Attempt.failure(Err(s"Expected ${types.size} elements, but got ${cql}"))
        else {
          for {
            h <- CType[V].parse(cql.head) //safe since the guard above
            t <- HListCType[L].parse(cql.tail)
          } yield h :: t
        }
      }

      def format(value: V :: L): Attempt[Seq[String]] = {
        for {
          h <- CType[V].format(value.head)
          t <- HListCType[L].format(value.tail)
        } yield h +: t
      }
    }
  }



}
