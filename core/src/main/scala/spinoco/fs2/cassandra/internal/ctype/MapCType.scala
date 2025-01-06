package spinoco.fs2.cassandra.internal.ctype

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.{DataType, DataTypes}
import com.datastax.oss.driver.internal.core.`type`.codec.ParseUtils
import scodec.bits.{BitVector, ByteVector}
import scodec.{Attempt, Codec, DecodeResult, Err, SizeBound}
import spinoco.fs2.cassandra.{CType, MapKeyCType, util}

import scala.annotation.tailrec

object MapCType {

  def instance[K : MapKeyCType, V : CType]: CType[Map[K,V]] = {
    new CType[Map[K, V]] {
      def cqlType: DataType =
        DataTypes.mapOf(MapKeyCType[K].cqlType, CType[V].cqlType)

      def cqlCodec(protocolVersion: ProtocolVersion): Codec[Map[K, V]] =
        MapCType.cqlCodec[K, V](protocolVersion)

      def format(a: Map[K, V]): Attempt[String] =
        MapCType.format(a)

      def parse(from: String): Attempt[Map[K, V]] =
        MapCType.parse[K, V](from)

    }
  }

  /**
    * Formats the map to CQL type serialized string form
    */
  def format[K : MapKeyCType, V : CType](a: Map[K, V]): Attempt[String] = {
    @tailrec
    def go(rem: Map[K, V], acc: String): Attempt[String] = {
      rem.headOption match {
        case None => Attempt.successful("{" + acc + "}")
        case Some((k,v)) =>
          MapKeyCType[K].format(k) match {
            case Attempt.Successful(ks) =>
              CType[V].format(v) match {
                case Attempt.Successful(vs) =>
                  val formatted = s"$ks : $vs"
                  if (acc.isEmpty) go(rem.tail, formatted)
                  else go(rem.tail, acc + "," + formatted)

                case Attempt.Failure(err) =>
                  Attempt.Failure(Err.General(s"Failed to format value: $err", err.message +: err.context))
              }
            case Attempt.Failure(err) =>
              Attempt.Failure(Err.General(s"Failed to format key: $err", err.message +: err.context))
          }
      }
    }
    go(a, "")
  }

  /**
    * Produces a codec with supplied version allowing to encode/decode from the bytes on the Cassandra wire
    * @param protocolVersion Version of the C* protocol to use
    */
  def cqlCodec[K : MapKeyCType, V : CType](protocolVersion: ProtocolVersion): Codec[Map[K, V]] = {
    // An int indicating the number of key/value pairs in the map, followed by the pairs. Each pair
    // is a byte array representing the serialized key, preceded by an int indicating its size,
    // followed by the value in the same format.

    // key must not be null in the map, so we always require size to be >= 0
    val keyCodec =
      scodec.codecs.variableSizeBytes(scodec.codecs.int32, MapKeyCType[K].cqlCodec(protocolVersion))

    // values may have nulls encoded as -1 length so we use element codec instead
    val valueCodec =
      codecs.elementCodec(CType[V].cqlCodec(protocolVersion))

    new Codec[Map[K, V]] {
      def encode(value: Map[K, V]): Attempt[BitVector] = {
        @tailrec
        def go(remains: Map[K, V], acc: BitVector): Attempt[BitVector] = {
          remains.headOption match {
            case None => Attempt.successful(acc)
            case Some((k, v)) =>
              keyCodec.encode(k) match {
                case Attempt.Successful(serializedK) =>
                  valueCodec.encode(v) match {
                    case Attempt.Successful(serializedV) =>
                      go(remains.tail, acc ++ serializedK ++ serializedV)

                    case Attempt.Failure(err) =>
                      Attempt.failure(Err.General(s"Failed to serialize value: $v at key $k", err.message +: err.context))

                  }

                case Attempt.Failure(err) =>
                  Attempt.failure(Err.General(s"Failed to serialize key: $k", err.message +: err.context))
              }
          }
        }

        go(value, ByteVector.fromInt(value.size).bits)
      }

      def sizeBound: SizeBound = SizeBound.unknown

      def decode(bits: BitVector): Attempt[DecodeResult[Map[K, V]]] = {
        scodec.codecs.int32.decode(bits).flatMap { case DecodeResult(size, remainingBits) =>
          @tailrec
          def go(remains: Int, bits: BitVector, acc: Map[K, V]): Attempt[DecodeResult[Map[K, V]]] = {
            if (remains > 0) {
              keyCodec.decode(bits) match {
                case Attempt.Successful(DecodeResult(k, rest)) =>
                  valueCodec.decode(rest) match {
                    case Attempt.Successful(DecodeResult(v, rest)) =>
                      go(remains-1, rest, acc + (k -> v))

                    case Attempt.Failure(err) =>
                      Attempt.failure(Err.General(s"Failed to deserialize value for key[${size-remains}]]: $k: $err", err.message +: err.context))
                  }

                case Attempt.Failure(err) =>
                  Attempt.failure(Err.General(s"Failed to deserialize key[${size-remains}]: $err", err.message +: err.context))
              }
            } else {
              Attempt.successful(DecodeResult(acc, bits))
            }
          }
          go(size, remainingBits, Map.empty)
        }
      }

    }
  }

  /**
    * Perser to parse value form the string representation
    * @param from String to parse
    */
  def parse[K : MapKeyCType, V : CType](from: String): Attempt[Map[K, V]] = {
    if (from == null || from.isEmpty || from == "NULL") Attempt.successful(Map.empty)
    else {
      val start = ParseUtils.skipSpaces(from, 0)
      if (from.charAt(start) != '{') {
        Attempt.failure(Err(s"Invalid char at $start expected {, got ${from.charAt(start)}"))
      } else {
        @tailrec
        def go(acc:Map[K,V], idx:Int): Attempt[Map[K,V]] = {
          if (idx >= from.length ) Attempt.failure(Err(s"Missing closing character in CQL (}) : $from"))
          else {
            val startOfKey = ParseUtils.skipSpaces(from, idx)
            if (from.charAt(startOfKey) == '}') Attempt.successful(acc)
            else {
              val parseValueResult: Attempt[((K, V), Int)] =
                for {
                  endOfKey <- util.attempt(ParseUtils.skipCQLValue(from, startOfKey))
                  k <- MapKeyCType[K].parse(from.substring(startOfKey, endOfKey))
                  startSplit <- util.attempt(ParseUtils.skipSpaces(from, endOfKey))
                  _ <- if (from.charAt(startSplit) != ':') Attempt.failure(Err(s"Expected : at $startSplit but got ${from.charAt(startSplit)}"))
                  else Attempt.successful(())
                  startOfValue <- util.attempt(ParseUtils.skipSpaces(from, startSplit + 1))
                  endOfValue <- util.attempt(ParseUtils.skipCQLValue(from, startOfValue))
                  v <- CType[V].parse(from.substring(startOfValue, endOfValue))
                } yield (k -> v, endOfValue)

              parseValueResult match {
                case Attempt.Failure(err) => Attempt.failure(err)
                case Attempt.Successful((kv,next)) =>  go(acc + kv, next)
              }
            }
          }
        }

        go(Map.empty,start+1)
      }

    }
  }

}
