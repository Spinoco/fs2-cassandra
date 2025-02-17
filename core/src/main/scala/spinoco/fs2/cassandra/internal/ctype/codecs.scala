package spinoco.fs2.cassandra.internal.ctype

import scodec.bits.BitVector
import scodec.{Attempt, Codec, DecodeResult, Err, SizeBound}

object codecs {

  /**
    * This is similar to variableSizeBytes but also handles -1 as an empty string on read.
    * On encode we should not receive null. And so the negative length is not used.
    * On read, we may receive -1 ans it is then threat as "0" so we pass
    * decision on null conversion to upper coded (emtpy string, 0, empty collection)
    * @param codec
    * @tparam A
    * @return
    */
  def elementCodec[A](codec: Codec[A]): Codec[A] = {
    new Codec[A] {
      def encode(value: A): Attempt[BitVector] = {
        codec.encode(value).flatMap { encoded =>
        scodec.codecs.int32.encode((encoded.size/8).toInt).map { size =>
          size ++ encoded
        }}
      }

      def sizeBound: SizeBound = SizeBound.atLeast(32) // at least 4 bytes for the length

      def decode(bits: BitVector): Attempt[DecodeResult[A]] = {
        scodec.codecs.int32.decode(bits).flatMap { case DecodeResult(size, rest) =>
          if (size == -1 || size == 0)
            codec.decode(BitVector.empty).map { case DecodeResult(a, _) => DecodeResult(a, rest) } // allow to decode null/empty string
          else {
            val (toDecode, remaining) = rest.splitAt(size*8)
            codec.decode(toDecode).flatMap { case DecodeResult(value, rest) =>
              if (rest.isEmpty) Attempt.successful(DecodeResult(value, remaining))
              else Attempt.failure(Err(s"Remaining bits after decoding a value in elementCodec: $value [$rest]"))
            }
          }
        }
      }
    }
  }

}
