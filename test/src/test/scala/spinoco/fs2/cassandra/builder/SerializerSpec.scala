package spinoco.fs2.cassandra.builder

import scodec.bits.BitVector
import shapeless.Witness
import spinoco.fs2.cassandra.internal.ctype.MapCType
import spinoco.fs2.cassandra.support.Fs2CassandraSpec
import spinoco.fs2.cassandra.MapKeyCType._
import spinoco.fs2.cassandra.internal.CodecSerializer.CodecSerializeSyntax
import spinoco.fs2.cassandra.internal.keyOf
import shapeless.labelled.FieldType
import shapeless.syntax.singleton._
import shapeless.labelled.{FieldType, KeyTag}

class SerializerSpec  extends Fs2CassandraSpec {

  val data: List[Int] = List(0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x05, 0x63, 0x6C, 0x61, 0x73, 0x73, 0x00, 0x00, 0x00, 0x2B, 0x6F, 0x72, 0x67, 0x2E, 0x61, 0x70, 0x61, 0x63,
  0x68, 0x65, 0x2E, 0x63, 0x61, 0x73, 0x73, 0x61, 0x6E, 0x64, 0x72, 0x61, 0x2E, 0x6C, 0x6F, 0x63, 0x61, 0x74, 0x6F, 0x72, 0x2E, 0x53, 0x69, 0x6D, 0x70, 0x6C, 0x65, 0x53, 0x74, 0x72, 0x61, 0x74, 0x65, 0x67, 0x79, 0x00, 0x00, 0x00, 0x12, 0x72, 0x65, 0x70, 0x6C, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6F, 0x6E, 0x5F, 0x66, 0x61,
  0x63, 0x74, 0x6F, 0x72, 0x00, 0x00, 0x00, 0x01, 0x31)

  def toHexString(bytes: List[Int]): String = bytes.map("%02X" format _).mkString

  val mapSample: BitVector = BitVector.fromValidHex(toHexString(data))

  val ctype = MapCType.instance[String, String]

  val wt = Witness.mkWitness("\"[applied]\"")

//  val wt = QuoteFieldAux.

  type IfExistsField = Witness.`"[applied]"`.->>[Boolean]


  "TEST SERIALIZERS" - {

    "Test?" in {
      println("===== TESTING =====")
      println(s"$mapSample")

      ctype.deserialize(mapSample.toByteBuffer, null)

      1 shouldBe { 1 }
    }

    "Quote test" in {
      val k = keyOf(wt)
      k shouldBe "\"[applied]\""

//      val k2 = keyOf(wt2.leftSideValue.witness)
//      k2 shouldBe "\"[applied]\""
    }

  }

}
