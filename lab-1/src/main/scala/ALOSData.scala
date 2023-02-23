import org.apache.spark.sql.types._

object ALOSSchema {
  val schema =
    StructType(
      Array(
        StructField("lat", DoubleType, nullable=true),
        StructField("lon", DoubleType, nullable=true),
        StructField("elevation", IntegerType, nullable=true)
      )
    )
}

case class ALOSData (
  lat: Double,
  lon: Double,
  elevation: Int
)
