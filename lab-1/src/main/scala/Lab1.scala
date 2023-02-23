import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.LongType


object Lab1 {
  def main(args: Array[String]) {

    // Set log level
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val rand = new scala.util.Random

    val (defaultParallelism, shuffleParallelism, orcPath, parquetPath, seaLevelRise, tempResultsPath) = ArgumentParser.getArguments(args)
    val runKey = rand.nextInt(1000000).toString
    val resultsPath = tempResultsPath + runKey + "/"

    val relocationsPath = resultsPath + "relocations"
    val destinationsPath = resultsPath + "destinations"

    // Create a SparkSession
    val spark = SparkSession.builder.appName("Lab 1").getOrCreate
    spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
    spark.conf.set("spark.sql.orc.filterPushdown", "true")
    spark.conf.set("spark.default.parallelism", defaultParallelism)
    spark.conf.set("spark.sql.shuffle.partitions", shuffleParallelism)

    import spark.implicits._

    // Filter OSM to get all data we need for the query, and cache result
    val placeTags: List[String] = List("city", "town", "village", "hamlet")
    val filteredOSM = spark.read
      .schema(Schemas.nodeSchema)
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .orc(orcPath)
      .as[Nodes]
      .select("id", "tags", "lat", "lon")
      .filter(
        ((col("tags").getItem("place").isInCollection(placeTags)
          && col("tags").getItem("population").isNotNull) ||
          col("tags").getItem("harbour") === "yes")
          && (col("lat").isNotNull && col("lon").isNotNull)
      )
      .withColumn("h3", UDF.toH3(col("lat"), col("lon")))
    Cacher.cache(filteredOSM)

    // Get all data points concerning cities, towns, villages and hamlets
    val allPlaces = filteredOSM.filter(col("tags").getItem("place").isInCollection(placeTags)
        && col("tags").getItem("population").isNotNull)
      .select(
        col("id"),
        col("tags").getItem("name").as("name"),
        col("tags").getItem("population").as("population").cast(LongType),
        col("h3"),
        col("lat"),
        col("lon"))

    // Read all ALOS data with elevation higher than seaLevelRise - 1
    val alos = spark.read
      .schema(ALOSSchema.schema)
      .load(parquetPath)
      .as[ALOSData]

    // Calculate H3's for ALOS dataset
    val alosWithH3 = alos.withColumn("h3", UDF.toH3(col("lat"), col("lon")))

    // Calculate the average elevation for each h3. We do this with a groupBy, which in this case
    // is a necessary shuffle because equal h3's could be on different worker nodes.
    val h3ToElevation = alosWithH3.groupBy("h3").avg("elevation")
    val h3ToSafe = h3ToElevation.filter(col("avg(elevation)") >= seaLevelRise)
    Cacher.cache(h3ToSafe)

    // Get all places that need to relocate. We do this with a join operation, which is in this case
    // is a necessary shuffle because rows in allPlaces and rows in h3ToSafe that match on their h3
    // column might be on different worker nodes, hence the data must be shuffled.
    val placesToRelocate = allPlaces.join(h3ToSafe, Seq("h3"), "leftanti")
      .select(
        col("id"),
        col("population"),
        col("lat"),
        col("lon")
      )
    Cacher.cache(placesToRelocate)
    val placesToRelocateWithoutPop = placesToRelocate.drop("population")

    // Get all cities
    val allCities = allPlaces.filter(col("tags").getItem("place") === "city")

    // Get all safe cities, so above sea level. We do this with a join operation, which is in this case
    // is a necessary shuffle because rows in allPlaces and rows in h3ToSafe that match on their h3
    // column might be on different worker nodes, hence the data must be shuffled.
    val safeCities = h3ToSafe
      .join(allCities, Seq("h3"), "inner")
      .select(col("id").as("destinationId"),
        col("name").as("destination"),
        col("population").as("destinationPopulation"),
        col("lat"),
        col("lon")
      )
    Cacher.cache(safeCities)
    val safeCitiesMinimal = safeCities.select(
      col("destinationId"),
      col("lat"),
      col("lon")
    )

    // Get harbours
    val harbours = filteredOSM
      .filter(col("tags").getItem("harbour") === "yes")
      .select(
        col("lat"),
        col("lon")
      )

    // Get the placesToRelocate x safeCities rows with the minimum distance between them
    // Note that this function causes shuffles, see the function code on when and why.
    val placesToRelocateClosestSafeCity = API.matchOnMinDistance(placesToRelocateWithoutPop, safeCitiesMinimal, "id")
      .withColumnRenamed("distance", "cityDistance")

    // Get the placesToRelocate x harbours rows with the minimum distance between them
    // Note that this function causes shuffles, see the function code on when and why.
    val placesToRelocateClosestHarbour = API.matchOnMinDistance(placesToRelocateWithoutPop, harbours, "id")
      .withColumnRenamed("distance", "harbourDistance")

    // Combine results and re-add population column. We do this with two join operations, which is in this case
    // are necessary shuffles because rows in placesToRelocateClosestSafeCity, rows in placesToRelocateClosestHarbour,
    // and rows in placesToRelocate that match on the name might be on different worker nodes, hence the data must be shuffled.
    val placesClosestSafeCityClosestHarbour = placesToRelocateClosestSafeCity
      .join(placesToRelocateClosestHarbour, Seq("id"), "inner")
      .join(placesToRelocate, Seq("id"), "inner")
      .select(
        col("id"),
        col("population"),
        col("destinationId")
      )

    // Split based on harbourDistance vs. cityDistance
    val (placesSafeCitySafeHarbour, placesSafeCityUnsafeHarbour) =
      API.split(placesClosestSafeCityClosestHarbour, col("harbourDistance") < col("cityDistance"))

    // Generate dataset for relocations when there is no close harbour
    val resultsCityUnsafeHarbour = placesSafeCityUnsafeHarbour
      .select(
        col("id"),
        col("population").as("num_evacuees"),
        col("destinationId")
      )

    // Generate dataset for partial relocation to the city when there is a close harbour
    val resultsCitySafeHarbour = placesSafeCitySafeHarbour
      .withColumn("cityRelocationPopulation", round(col("population") * 0.75))
      .select(
        col("id"),
        col("cityRelocationPopulation").as("num_evacuees").cast(LongType),
        col("destinationId")
      )

    // Generate dataset for partial relocation to the harbour when there is a close harbour
    val resultsHarbourSafeHarbour = placesSafeCitySafeHarbour
      .withColumn("harbourRelocationPopulation", round(col("population") * 0.25))
      .withColumn("harbourId", lit(-1))
      .select(
        col("id"),
        col("harbourRelocationPopulation").as("num_evacuees").cast(LongType),
        col("harbourId").as("destinationId")
      )

    // Merge datasets
    val relocations = resultsCityUnsafeHarbour
      .union(resultsCitySafeHarbour.union(resultsHarbourSafeHarbour))
    Cacher.cache(relocations)

    // Generate dataset including old and new population of each destination
    // We do this with a groupBy operation, which causes a shuffle.
    // This is a necessary shuffle because different rows with the same value for destination could be
    // on different worker nodes, so to find the sum of all evacuees across different worker nodes, we
    // have to shuffle data around.
    val destinationToNumEvacuees = relocations
      .groupBy("destinationId")
      .sum("num_evacuees")

    // Get the old population and name from the safeCities dataset.
    // We do this with a join operation, which is in this case
    // is a necessary shuffle because rows in destinationToNumEvacuees and rows in safeCities that match on the destination
    // column might be on different worker nodes, hence the data must be shuffled.
    val destinations = destinationToNumEvacuees
      .join(safeCities, Seq("destinationId"), "left")
      .na.fill(0)
      .na.fill("Waterworld")
      .select(
        col("destination"),
        col("destinationPopulation").as("old_population"),
        (col("destinationPopulation") + col("sum(num_evacuees)")).as("new_population")
      )

    // Re-add name column.
    // We do this with a join operation, which in this case
    // is a necessary shuffle because rows in the allPlaces and rows in the relocations that match on the id column
    // might be on different worker nodes, hence the data must be shuffled.
    // Note that this shuffle is a necessary evil, because there are places with the same name, so we have to use ID.
    // Taking the name column through the whole process of calculating the relocations and destinations would have taken
    // much more network I/O.
    val relocationsWithName = relocations
      .join(allPlaces, Seq("id"), "left")
      .join(safeCities, Seq("destinationId"), "left")
      .na.fill("Waterworld")
      .select(
        col("name").as("place"),
        col("num_evacuees"),
        col("destination")
      )

    relocationsWithName.write.format("orc").save(relocationsPath)
    destinations.write.format("orc").save(destinationsPath)

    // Unpersist all caches
    Cacher.uncacheAll()

    // Stop the underlying SparkContext
    spark.stop
  }
}
