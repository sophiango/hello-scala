import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object NetflixJoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NetflixJoin")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .master("local[*]")
      .getOrCreate()

    val movies = spark.createDataFrame(Seq(
      ("m1", "Inception", "Sci-Fi", 2010, 148),
      ("m2", "Interstellar", "Sci-Fi", 2014, 169),
      ("m3", "The Dark Knight", "Action", 2008, 152),
      ("m4", "Memento", "Thriller", 2000, 113)
    )).toDF("movie_id", "title", "genres", "year", "duration")

    val ratings = spark.createDataFrame(Seq(
      ("u1", "m1", 5),
      ("u2", "m1", 4),
      ("u3", "m2", 5),
      ("u4", "m3", 4),
      ("u5", "m4", 3),
      ("u1", "m2", 4)
    )).toDF("user_id", "movie_id", "rating")

    val joined = movies.join(ratings, "movie_id")

    // average rating per movie title
    val avgRating = joined.groupBy("title", "genres").agg(avg(col("rating")).alias("avg_rating"))
    println("=====average rating per movie title=====")
    avgRating.show()

    // ranking movies by rating using window
    val windowSpec = Window.partitionBy("genres").orderBy(col("avg_rating").desc)
    val ranked = avgRating.withColumn("rank", row_number().over(windowSpec))
    println("=====ranking movies by rating using window=====")
    ranked.show()

    // longest movies in each genres
    val longestMovie = joined.groupBy("genres").agg(max(col("duration"))).limit(1)
    println("=====longest movies in each genres=====")
    longestMovie.show()

    // distinct user who rated each movie
    val distinctUsers = joined.groupBy("title").agg(countDistinct("user_id").alias("num_users"))
    println("=====distinct user who rated each movie=====")
    distinctUsers.show()

    // users who rated more than 1 movies
    val activeUsers = joined.groupBy("user_id")
      .agg(count("movie_id").alias("rating_count"))
      .filter(col("rating_count") > 1)
    println("=====users who rated more than 1 movies=====")
    activeUsers.show()

    // most common genres
    val explodedGenres = joined.withColumn("genre", explode(split(col("genres"), ",\\s*")))
    val genreCount = explodedGenres.groupBy("genre").agg(countDistinct("title").alias("num_movies")).orderBy(col("num_movies").desc)
    println("=====most common genres=====")
    genreCount.show()
    spark.stop()
  }
}
