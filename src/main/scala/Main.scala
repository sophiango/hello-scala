//
//import org.apache.spark.sql.SparkSession
//
//object Main {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder
//      .appName("FirstApp")
//      .master("local[*]")
//      .config("spark.driver.bindAddress", "127.0.0.1")
//      .config("spark.driver.host", "127.0.0.1")
//      .getOrCreate()
////    val data = Seq(("Alice", 23), ("Bob", 45))
////    val df = spark.createDataFrame(data).toDF("Name", "Age")
//    val taxiDF = spark.read.parquet("data/yellow_tripdata_2023-01.parquet")
//    taxiDF.printSchema()
//    println("=== Sample data ===")
//    taxiDF.show(5)
//
//    // count rows
//    println(s"total record: ${taxiDF.count()}")
//
//    // average
//    val avgTripByPassenger = taxiDF.groupBy("passenger_count").avg("trip_distance")
//    avgTripByPassenger.show()
//
//    spark.stop()
//  }
//}
//
