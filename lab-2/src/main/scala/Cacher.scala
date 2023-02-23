import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable.ListBuffer

object Cacher {
  var cached = ListBuffer[Dataset[Row]]()

  def cache(dataset : Dataset[Row]): Unit = {
    dataset.cache()
    cached += dataset
  }

  def uncacheAll(): Unit = {
    cached.foreach(
      x => x.unpersist()
    )
  }
}
