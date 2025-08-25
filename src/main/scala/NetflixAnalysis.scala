import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object NetflixAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Netflix")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()

    // step 1: load csv
    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/netflix_titles.csv")

    println("Schema")
    df.printSchema()

    // remove data if release_year or country is null
    val cleanedDF = df.na.drop(Seq("country", "release_year"))

    // movies release after 2010
    val twentyTenMovies = cleanedDF.filter(col("type") === "Movie" && col("release_year") === 2010)
    twentyTenMovies.show()

    // top 10 categories
    val explodedGenres = cleanedDF.withColumn("genre", explode(split(col("listed_in"), ",\\s*")))
    val topTenGenres = explodedGenres.groupBy("genre").count().orderBy(col("count").desc)
    topTenGenres.show()

    // movies and tvshow by country
    val contentByCountry = cleanedDF.groupBy("country", "type").count()
    contentByCountry.show()

    // yearly trends: how many movies and tv shows release each year
    val yearlyRelease = cleanedDF.groupBy("country", "release_year").count().orderBy("release_year", "type")
    yearlyRelease.show()

    // write to parquet instead of show()
    val movieDF = cleanedDF.filter(col("type") === "Movie")
    movieDF.write.partitionBy("release_year").parquet("output/movies_by_year")

    spark.stop()
  }
}
