package spinoco.fs2.cassandra.ctype.types

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.internal.core.`type`.codec.ParseUtils
import scodec.bits.BitVector
import scodec.{Attempt, Codec, DecodeResult, Err, SizeBound}
import spinoco.fs2.cassandra.baseutil
import spinoco.fs2.cassandra.ctype.{CType, CollectionType}

import scala.annotation.tailrec

object CollectionCType {

  case class ConstDimension(dimension: Int, tpe: DataType)

  def instance[C[_]: CollectionType, A: CType](dimension: Option[ConstDimension]): CType[C[A]] = {
    new CType[C[A]] {
      def cqlType: DataType = dimension.map(_.tpe).getOrElse(CollectionType[C].cqlType(CType[A].cqlType))

      def cqlCodec(protocolVersion: ProtocolVersion): Codec[C[A]] =
        CollectionCType.cqlCodec[C, A](protocolVersion, dimension = dimension.map(_.dimension))

      def format(a: C[A]): Attempt[String] =
        CollectionCType.format(a)

      def parse(cql: String): Attempt[C[A]] =
        CollectionCType.parse(cql)
    }
  }


  /**
    * Builds a codec of the collection for the CQL
    * @param protocolVersion  version of the protocol for the codec
   *
   * Note:
   * - In cassandra, empty collection and null is the same thing. Therefore, Insert Some(List.empty) will return None on select,
   *   as Option takes precedence.
    */
  def cqlCodec[C[_]: CollectionType, A: CType](protocolVersion: ProtocolVersion, dimension: Option[Int]): Codec[C[A]] = {

    /**
     * By default [elementCount (int32), [elem1Size(int32), [ ... ] ], [elem2size(int32), [...] ], ... ]
     * If dimension is constant, then just the [ ... ] data from above.
     */

    val elemDataCodec = CType[A].cqlCodec(protocolVersion)
    val elementCodec = if (dimension.isEmpty) { codecs.elementCodec(elemDataCodec) } else { elemDataCodec }

    new Codec[C[A]] {
      def encode(value: C[A]): Attempt[BitVector] = {
        @tailrec
        def go(remains: C[A], acc: BitVector): Attempt[BitVector] = {
          CollectionType[C].uncons1(remains) match {
            case None => Attempt.successful(acc)
            case Some((head, tail)) =>
              elementCodec.encode(head) match {
                case Attempt.Successful(encodedBits) => go(tail, acc ++ encodedBits)
                case Attempt.Failure(err) => Attempt.failure(Err.General(s"Failed to encode element: $head", err.message +: err.context))
              }
          }
        }

        go(value, BitVector.empty).map { elements =>
          if (dimension.isEmpty) {
            val elementCount = BitVector.fromInt(CollectionType[C].sizeOf(value))
            elementCount ++ elements
          } else {
            elements
          }
        }
      }

      def sizeBound: SizeBound = SizeBound.unknown

      def decodeSize(bits: BitVector): Attempt[DecodeResult[Int]] = {
        dimension.fold(
          /** if this is a variable sized container, read the size from the bit vector */
          scodec.codecs.int32.decode(bits)
        ) (
          /** if dimension is constant, return dimension */
          d => Attempt.successful(DecodeResult(d, bits))
        )
      }

      def decode(bits: BitVector): Attempt[DecodeResult[C[A]]] = {
        if (bits == null || bits.isEmpty) Attempt.Successful(DecodeResult(CollectionType[C].zero, bits))
        else {
          decodeSize(bits).flatMap { case DecodeResult(elementCount, bits) =>
            def go(remains: Int, remainsBits: BitVector, acc: C[A]): Attempt[DecodeResult[C[A]]] = {
              if (remains > 0) {
                elementCodec.decode(remainsBits) match {
                  case Attempt.Successful(DecodeResult(element, rest)) =>
                    go(remains - 1, rest, CollectionType[C].append(acc, element))

                  case Attempt.Failure(err) =>
                    Attempt.failure(Err.General(s"Failed to decode element at ${elementCount - remains}", err.message +: err.context))
                }
              } else {
                Attempt.successful(DecodeResult(acc, remainsBits))
              }
            }
            go(elementCount, bits, CollectionType[C].zero)
          }
        }
      }
    }
  }

  /**
    * Formats collection with CQL syntax [elem1, elem2, elem3]
    */
  def format[C[_] : CollectionType, A : CType](ca: C[A]): Attempt[String] = {
    def go(remains: C[A], acc: String): Attempt[String] = {
      CollectionType[C].uncons1(remains) match {
        case None => Attempt.Successful(s"[$acc]")
        case Some((head, tail)) =>
          CType[A].format(head) match {
            case Attempt.Successful(formatted) =>
              if (acc.isEmpty) go(tail, formatted)
              else go(tail, acc +  "," + formatted)

            case Attempt.Failure(err) =>
              Attempt.failure(Err.General(s"Failed to format element: $head", err.message +: err.context))

          }
      }
    }

    go(ca, "")
  }

  /**
    * Simple parser of the CQL collection value from the string
    * @param cql             Source CQL string
    */
  def parse[C[_]: CollectionType, A: CType](cql: String): Attempt[C[A]] = {
    if (cql.trim.isEmpty || cql.trim == "NULL") Attempt.successful(CollectionType[C].zero)
    else {
      val start = ParseUtils.skipSpaces(cql, 0)
      if (cql.charAt(start) != '[') {
        Attempt.Failure(Err(s"Invalid char at $start expected '[', got ${cql.charAt(start)}"))
      } else {
        @tailrec
        def go(acc:C[A], idx:Int): Attempt[C[A]] = {
          if (idx >= cql.length ) Attempt.Failure(Err(s"Missing closing character in CQL : $cql"))
          else {
            val start2 = ParseUtils.skipSpaces(cql, idx)
            if (cql.charAt(start2) == ']') Attempt.successful(acc)
            else {
              val parseValueResult =
                for {
                  endOfCql <- baseutil.attempt(ParseUtils.skipCQLValue(cql, start2))
                  parsed <- CType[A].parse(cql.substring(start2, endOfCql))
                } yield (parsed, endOfCql)

              parseValueResult match {
                case Attempt.Failure(err) => Attempt.Failure(err)
                case Attempt.Successful((a,next)) => go(CollectionType[C].append(acc,a), next)
              }
            }
          }
        }

        go(CollectionType[C].zero, start + 1) // skip the opening char already
      }
    }
  }
}
