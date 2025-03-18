package spinoco.fs2.cassandra.ctype.types

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.internal.core.`type`.codec.IntCodec
import scodec.bits.BitVector
import scodec.{Attempt, Codec, DecodeResult, SizeBound}
import spinoco.fs2.cassandra.baseutil
import spinoco.fs2.cassandra.ctype.CType

object IntCType {

  val instance: CType[Int] = {
    val nativeCodec = new IntCodec
    new CType[Int] {
      def cqlType: DataType = nativeCodec.getCqlType

      def cqlCodec(protocolVersion: ProtocolVersion): Codec[Int] = {
        new Codec[Int] {
          def encode(value: Int): Attempt[BitVector] =
            scodec.codecs.int32.encode(value)

          val sizeBound: SizeBound =
            SizeBound.choice(Seq(
              SizeBound.exact(0) // b/c empty is allowed too
              , scodec.codecs.float.sizeBound
            ))

          def decode(bits: BitVector): Attempt[DecodeResult[Int]] = {
            if (bits.isEmpty) Attempt.successful(DecodeResult(0, BitVector.empty))
            else scodec.codecs.int32.decode(bits)
          }
        }
      }

      def parse(cql: String): Attempt[Int] =
        baseutil.attempt(nativeCodec.parse(cql) : Int)

      def format(a: Int): Attempt[String] =
        baseutil.attempt(nativeCodec.format(a))
    }
  }

}
