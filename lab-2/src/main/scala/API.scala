import org.apache.spark.sql._
import org.apache.spark.sql.functions.col

object API {

  def split(set: Dataset[Row], condition: Column): (Dataset[Row], Dataset[Row]) = {
    Cacher.cache(set)
    val trueSet = set.filter(condition)
    val falseSet = set.filter(!condition)

    (trueSet, falseSet)
  }

  def selectDistinct(set: Dataset[Row], columnNames : String*): Dataset[Row] = {
    set.select(columnNames.map(x => col(x)): _*).distinct()
  }

  def matchOnMinDistance(set: Dataset[Row], set2: Dataset[Row], groupByColumn : String): Dataset[Row] = {
    val set2Renamed = set2
      .withColumnRenamed("lat", "lat2")
      .withColumnRenamed("lon", "lon2")

    // In order to calculate distances between each possible combination of rows,
    // we have to calculate a cartesian product of the two sets, which causes a shuffle
    // because both sets are distributed on worker nodes, so we have to shuffle data
    // around to combine every row from set with every row from set2Renamed.
    val setsWithDistance = set
      .crossJoin(set2Renamed)
      .withColumn("distance", UDF.calcDistance(col("lat"), col("lon"), col("lat2"), col("lon2")))
      .drop("lat", "lon", "lat2", "lon2")

    // We return the row with the minimum distance from the cartesian product.
    // Note that this function causes shuffles, see the function code on when and why.
    API.selectMinRow(setsWithDistance, groupByColumn, "distance")
  }

  def selectMinRow(set: Dataset[Row], groupByColumn : String, minColumn : String) : Dataset[Row] = {
    // In order to find the minimum distance per groupByColumn, we need to perform a groupBy operation,
    // which causes a shuffle because different rows with the same value for groupByColumn could be
    // on different worker nodes, so to find the minimum distance across different worker nodes, we
    // have to shuffle data around.
    val setGrouped = set
    .groupBy(groupByColumn).min(minColumn)
    .withColumnRenamed(groupByColumn, "temp")

    val minColumnName = "min(" + minColumn + ")"

    // The groupBy operation only returns the groupByColumn and the result of the aggregation (in this case
    // the minColumn, but we need everything from the original dataset. Hence we have to rejoin the setGrouped
    // with the original set, which causes a shuffle. This is a necessary shuffle because the data from the
    // groupBy operation could be on a different worker node than the original data.
    val minSelected = set.join(setGrouped, set(groupByColumn) === setGrouped("temp")
      && set(minColumn) === setGrouped(minColumnName), "inner")
      .drop("temp", minColumnName)
    minSelected
  }
}
