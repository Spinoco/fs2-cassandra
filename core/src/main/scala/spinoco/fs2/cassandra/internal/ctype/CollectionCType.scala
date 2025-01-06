package spinoco.fs2.cassandra.internal.ctype

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.internal.core.`type`.codec.ParseUtils
import scodec.bits.BitVector
import scodec.{Attempt, Codec, DecodeResult, Err, SizeBound}
import spinoco.fs2.cassandra.{CType, CollectionType, util}

import scala.annotation.tailrec

object CollectionCType {

  def instance[C[_] : CollectionType, A : CType]: CType[C[A]] = {
    new CType[C[A]] {
      def cqlType: DataType = CollectionType[C].cqlType(CType[A].cqlType)

      def cqlCodec(protocolVersion: ProtocolVersion): Codec[C[A]] =
        CollectionCType.cqlCodec[C, A](protocolVersion)

      def format(a: C[A]): Attempt[String] =
        CollectionCType.format(a)

      def parse(cql: String): Attempt[C[A]] =
        CollectionCType.parse(cql)
    }
  }


  /**
    * Builds a codec of the collection for the CQL
    * @param protocolVersion  version of the protocol for the codec
    */
  def cqlCodec[C[_] : CollectionType, A: CType](protocolVersion: ProtocolVersion): Codec[C[A]] = {
    // An int indicating the number of elements in the list, followed by the elements. Each element
    // is a byte array representing the serialized value, preceded by an int indicating its size.

    val elementCodec = codecs.elementCodec(CType[A].cqlCodec(protocolVersion))


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

        val elementCount = BitVector.fromInt(CollectionType[C].sizeOf(value))
        go(value, elementCount)
      }

      def sizeBound: SizeBound = SizeBound.unknown

      def decode(bits: BitVector): Attempt[DecodeResult[C[A]]] = {
        if (bits.isEmpty) Attempt.Successful(DecodeResult(CollectionType[C].zero, bits))
        else {
          scodec.codecs.int32.decode(bits).flatMap { case DecodeResult(count, bits) =>
            def go(remains: Int, remainsBits: BitVector, acc: C[A]): Attempt[DecodeResult[C[A]]] = {
              if (remains > 0) {
                elementCodec.decode(remainsBits) match {
                  case Attempt.Successful(DecodeResult(element, rest)) =>
                    go(remains - 1, rest, CollectionType[C].append(acc, element))
                  case Attempt.Failure(err) =>
                    Attempt.failure(Err.General(s"Failed to decode element at ${count - remains}", err.message +: err.context))
                }
              } else {
                Attempt.successful(DecodeResult(acc, remainsBits))
              }
            }
            go(count, bits, CollectionType[C].zero)
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
    * @param collectionType  Type of the collection
    * @tparam C
    * @tparam A
    * @return
    */
  def parse[C[_] : CollectionType, A : CType](cql: String): Attempt[C[A]] = {
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
                  endOfCql <- util.attempt(ParseUtils.skipCQLValue(cql, start2))
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
