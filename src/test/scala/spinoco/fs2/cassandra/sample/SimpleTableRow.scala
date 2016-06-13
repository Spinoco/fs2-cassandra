package spinoco.fs2.cassandra.sample

import java.net.InetAddress
import java.util.UUID

import fs2.Chunk
import fs2.Chunk.Bytes
import shapeless.tag
import shapeless.tag._
import spinoco.fs2.cassandra.CType.{Ascii, Type1}

import scala.concurrent.duration.FiniteDuration


case class SimpleTableRow(
  intColumn: Int
  , longColumn: Long
  , stringColumn: String
  , asciiColumn: String @@ Ascii
  , floatColumn: Float
  , doubleColumn: Double
  , bigDecimalColumn: BigDecimal
  , bigIntColumn: BigInt
  , blobColumn: Bytes
  , uuidColumn: UUID
  , timeUuidColumn: UUID @@ Type1
  , durationColumn: FiniteDuration
  , inetAddressColumn: InetAddress
  , enumColumn: TestEnumeration.Value
)

object SimpleTableRow {
  import com.datastax.driver.core.utils.UUIDs.timeBased

  val simpleInstance = SimpleTableRow(
    intColumn = 1
    , longColumn = 2
    , stringColumn = "varchar string"
    , asciiColumn = tag[Ascii]("ascii string")
    , floatColumn =  1.1f
    , doubleColumn =  2.2d
    , bigDecimalColumn = BigDecimal(0.3d)
    , bigIntColumn = BigInt(3)
    , blobColumn = Chunk.bytes(Array.emptyByteArray)
    , uuidColumn =  timeBased
    , timeUuidColumn =  tag[Type1](timeBased)
    , durationColumn = FiniteDuration(1,"s")
    , inetAddressColumn = InetAddress.getLocalHost
    , enumColumn = TestEnumeration.One
  )

}