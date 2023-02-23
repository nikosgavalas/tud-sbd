object ArgumentParser {
    def getArguments(args: Array[String]) : (String, String, String, String,  Int, String) = {
      // Get the sea level
      if (args.length != 6) {
        println("Wrong number of arguments provided. Received " + args.length + " , expected: 6")
        for (i <- 0 to args.length - 1)
          println(args(i))
        println("Add arguments in the following order: (defaultParallelism, shuffleParallelism, orcPath, parquetPath, seaLevelRise, resultsPath).")
        sys.exit(1)
      }
      val defaultParallelism = args(0)
      val shuffleParallelism = args(1)
      val orcPath : String = args(2)
      val parquetPath : String = args(3)
      val seaLevelRise : Int = args(4).toInt
      val resultsPath : String = args(5)


      (defaultParallelism, shuffleParallelism, orcPath, parquetPath, seaLevelRise, resultsPath)
    }


}
