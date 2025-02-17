package spinoco.fs2.cassandra.serializers

import com.datastax.oss.driver.api.core.ProtocolVersion
import scodec.bits.BitVector
import spinoco.fs2.cassandra.CType
import spinoco.fs2.cassandra.support.Fs2CassandraSpec
import spinoco.fs2.cassandra.util.CodecSerializerSyntax.CodecSerializeDeserializeSyntax

import java.nio.ByteBuffer

class SerializerSpec  extends Fs2CassandraSpec {
  "Serializers" - {
    "Serialize empty string" in {
      val bb = CType
        .stringInstance
        .serialize("", ProtocolVersion.V6)
        .require

      BitVector(bb) shouldBe { BitVector.empty }
    }

    "Serialize none string" in {
      val bb = CType
        .optionInstance[String](CType.stringInstance)
        .serialize(None, ProtocolVersion.V6)
        .require

      bb shouldBe null
    }

    "Deserialize none string" in {
      val bb: ByteBuffer = null.asInstanceOf[ByteBuffer]
      val tpe = CType.optionInstance[String](CType.stringInstance)
      val res = tpe.deserialize(bb, ProtocolVersion.V6)

      res shouldBe Right(None)
    }

    "Deserialize empty string" in {
      val bb: ByteBuffer = BitVector.empty.toByteBuffer
      val tpe = CType.stringInstance
      val res = tpe.deserialize(bb, ProtocolVersion.V6)

      res shouldBe Right("")
    }

  }
}
