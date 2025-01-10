package spinoco.fs2.cassandra.internal.ctype

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.DataType
import scodec.Attempt.Successful
import scodec.{Attempt, Codec, SizeBound}
import spinoco.fs2.cassandra.CType
import spinoco.fs2.cassandra.util.BitVectorPrinter.BitVectorPrinterSyntax

object OptionCType {

  def instance[A : CType]:CType[Option[A]] = {
    new CType[Option[A]] {
      def cqlType: DataType = CType[A].cqlType

      def cqlCodec(protocolVersion: ProtocolVersion): Codec[Option[A]] = {
        new Codec[Option[A]] {
          val sizeBound: scodec.SizeBound =
            SizeBound.choice(Seq(
              SizeBound.exact(0)  // empty == None
              , CType[A].cqlCodec(protocolVersion).sizeBound
            ))

          def encode(value: Option[A]): Attempt[scodec.bits.BitVector] = {
            value match {
              /*
              case None => {

                if (cqlType.asCql(true, false) == "text") {
                  println(s"this is string!")
//                  CType[String].cqlCodec(protocolVersion).encode("")
                  // Successful(scodec.bits.BitVector.fromByte(0)) //empty string, we don't want this
                  // NO CType[Int].cqlCodec(protocolVersion).encode(-1)
                  Successful(scodec.bits.BitVector.empty) //empty string, we don't want this
                } else {
                  println(s"this is ${value.getClass}")
                  CType[Int].cqlCodec(protocolVersion).encode(-1)
                }
              }

               */
              case None => Successful(scodec.bits.BitVector.empty) //empty string, we don't want this
              case Some(a) => CType[A].cqlCodec(protocolVersion).encode(a)
            }
          }

          def decode(bits: scodec.bits.BitVector): Attempt[scodec.DecodeResult[Option[A]]] = {
            if (bits.isEmpty) Attempt.successful(scodec.DecodeResult(None, bits))
            else CType[A].cqlCodec(protocolVersion).decode(bits).map(_.map(Some(_)))
          }
        }
      }

      def parse(cql: String): Attempt[Option[A]] = {
        if (cql == null || cql.toUpperCase == "NULL") Attempt.successful(None)
        else CType[A].parse(cql).map(Some(_))
      }
      def format(a: Option[A]): Attempt[String] = a match {
        case None => Attempt.successful("NULL")
        case Some(a) => CType[A].format(a)
      }
    }
  }

}
