package spinoco.fs2.cassandra.serializers

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.codec.{TypeCodec, TypeCodecs}
import com.datastax.oss.driver.api.core.data.CqlVector
import scodec.{Attempt, DecodeResult}
import scodec.Attempt.Successful
import scodec.bits.BitVector
import shapeless.tag
import shapeless.tag.@@
import spinoco.KarelsTweaks.BitVectorPrinter.BitVectorPrinterSyntax
import spinoco.fs2.cassandra.CType
import spinoco.fs2.cassandra.internal.ctype.{CollectionCType, VectorCType}
import spinoco.fs2.cassandra.sample.VectorSizes.VectorSize4
import spinoco.fs2.cassandra.sample.VectorTableRow
import spinoco.fs2.cassandra.support.Fs2CassandraSpec
import spinoco.fs2.cassandra.util.CodecSerializerSyntax.CodecSerializeDeserializeSyntax

import java.nio.ByteBuffer
import java.util
import scala.collection.JavaConverters._


class SerializerSpec  extends Fs2CassandraSpec {
  "Serializers" - {
    "Serialize and deserialize vector" in {
      val data: Vector[Int] = Vector(1, 2, 3, 4)
      val dataAsTaggedVector: Vector[Int] @@ VectorSize4 = tag[VectorSize4](data)
      val dataAsCqlVector: CqlVector[Integer] = CqlVector.newInstance(data.map(java.lang.Integer.valueOf).asJava)

      val datastaxCodec = TypeCodecs.vectorOf(4, TypeCodecs.INT)
      val ourCodec = VectorCType.instance[Int, VectorSize4](4).cqlCodec(ProtocolVersion.V6)

      val datastaxSerialized = BitVector(datastaxCodec.encode(dataAsCqlVector, ProtocolVersion.V6))
      val ourSerialized = ourCodec.encode(dataAsTaggedVector)

      ourSerialized.require shouldBe datastaxSerialized

      val datastaxDeserialized = datastaxCodec.decode(datastaxSerialized.toByteBuffer, ProtocolVersion.V6)
      val ourDeserialized = ourCodec.decode(datastaxSerialized)

      datastaxDeserialized shouldBe dataAsCqlVector
      ourDeserialized shouldBe Attempt.successful(DecodeResult(dataAsTaggedVector, BitVector.empty))
    }

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
