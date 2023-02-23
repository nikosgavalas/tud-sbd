import com.uber.h3core.H3Core
import org.apache.spark.sql.functions.udf
import com.uber.h3core.H3Core
import org.apache.spark.sql.expressions.UserDefinedFunction

object UDF {
  object H3 extends Serializable {
    val instance = H3Core.newInstance()
    val RESOLUTION = 9
  }

  val toH3: UserDefinedFunction = udf((lat: Double, lon: Double) =>
    H3.instance.latLngToCellAddress(lat, lon, H3.RESOLUTION))

  val calcDistance: UserDefinedFunction = udf((lat: Double, lon: Double, lat2: Double, lon2: Double) =>
    (lat2 - lat) * (lat2 - lat) + (lon2 - lon) * (lon2 - lon))
}
