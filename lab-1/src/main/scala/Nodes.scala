import org.apache.spark.sql._
import org.apache.spark.sql.types._

import java.sql.Timestamp

object Schemas {
  val refSchema = StructType(
    StructField("ref", LongType, nullable=true) :: Nil
  )

  val memberSchema = StructType(
    StructField("type", StringType, nullable = true) ::
      StructField("ref", LongType, nullable = true) ::
      StructField("role", StringType, nullable = true) :: Nil
  )

  val nodeSchema =
    StructType(
      Array(
        StructField("id", LongType, nullable=true),
        StructField("type", StringType, nullable=true),
        StructField("tags", MapType(StringType, StringType), nullable=true),
        StructField("lat", DoubleType, nullable=true),
        StructField("lon", DoubleType, nullable=true),
        StructField("nds", ArrayType(refSchema), nullable=true),
        StructField("members", ArrayType(memberSchema), nullable=true),
        StructField("changeset", LongType, nullable=true),
        StructField("timestamp", TimestampType, nullable=true),
        StructField("uid", LongType, nullable=true),
        StructField("user", StringType, nullable=true),
        StructField("version", LongType, nullable=true),
        StructField("visible", BooleanType, nullable=true)
      )
    )
}

case class Nodes(
  id: Long,
  `type`: String,
  tags: Map[String, String],
  lat: Double,
  lon: Double,
  nds: Array[RefStruct],
  members: Array[MemberStruct],
  changeset: Long,
  timestamp: Timestamp,
  uid: Long,
  user: String,
  version: Long,
  visible: Boolean
)

case class RefStruct (
  ref: Long
)

case class MemberStruct(
  `type`: String,
  ref: Long,
  role: String
 )
